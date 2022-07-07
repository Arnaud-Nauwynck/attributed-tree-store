package fr.an.attrtreestore.azure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;

import com.azure.core.http.rest.PagedFlux;
import com.azure.storage.file.datalake.DataLakeDirectoryAsyncClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncParallelAzRecursiveListingToTree extends AzAsyncRecursiveListingToTree {

    private ExecutorService executorService;

    private int maxParallelProcessingTaskCount = 300;
    
    @Getter
    private final AtomicInteger submittedTaskCount = new AtomicInteger(0);

    @Getter
    private final AtomicInteger processingTaskCount = new AtomicInteger(0);

    @Getter
    protected LoggingCounter azQueryAsyncListPathCounter = new LoggingCounter("az query async dir.listPaths",
            new LoggingCounterParams(1_000_000, 60_000)); // every 1mn
    
    @Getter
    protected LoggingCounter totalSubmittedTaskCount = new LoggingCounter("submitted missing PathItem task", 
            new LoggingCounterParams(1_000_000, 300_000)); // every 5mn
    @Getter
    protected LoggingCounter totalProcessedTaskCount = new LoggingCounter("processed missing PathItem task",
            new LoggingCounterParams(1_000_000, 300_000)); // every 5mn

    // --------------------------------------------------------------------------------------------

    public AsyncParallelAzRecursiveListingToTree(NodeNameEncoder nameEncoder, IWriteTreeData destTree, //
            ExecutorService executorService
            ) {
        super(nameEncoder, destTree);
        this.executorService = executorService;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void executeListing(DataLakeDirectoryAsyncClient srcBaseDir, NodeNamesPath destBasePath) {
        long startTime = System.currentTimeMillis();
        int periodicDelayMillis = 10_000;
        log.info("... start executeListing (async tasks) " 
                + srcBaseDir.getDirectoryUrl()
                + " + periodic monitor progress every " + (periodicDelayMillis/1000) + "s");

        // => async launch tasks...
        // TODO currently blocking on recursive existing dirs
        super.executeListing(srcBaseDir, destBasePath);
        
        try {
            Thread.sleep(periodicDelayMillis);
        } catch (InterruptedException e) {
        }
        
        // wait until tasks are finished
        for(;;) {
            if (isInterrupt()) {
                log.info("interrupted executeListing");
                return;
            }
            int currSubmittedCount = submittedTaskCount.get();
            int currProcessingTaskCount = processingTaskCount.get();
            if (currSubmittedCount == 0 && currProcessingTaskCount == 0) {
                break; // finished!
            }
            log.info("curr dirListing tasks submitted: " + currSubmittedCount + " processing:" + currProcessingTaskCount);
            try {
                Thread.sleep(periodicDelayMillis);
            } catch (InterruptedException e) {
            }
        }
        
        long millis = System.currentTimeMillis() - startTime;
        log.info("... Finished executeListing " 
                + srcBaseDir.getDirectoryUrl()
                + ", took " + (millis/1000) + "s");
    }
    

    @Override
    protected void loopRecurseOnExistingDirDatas(
            DataLakeDirectoryAsyncClient dirClient, 
            NodeNamesPath destPath,
            Map<NodeName, NodeData> foundChildDatas) {
        if (foundChildDatas.size() == 1
                || (processingTaskCount.get() + 1 >= maxParallelProcessingTaskCount)
                ) {
            for(val foundChildData: foundChildDatas.values()) {
                val childName = foundChildData.name;
                val childPath = destPath.toChild(childName);
                if (foundChildData.type == NodeData.TYPE_DIR) {
                    doRecurseOnExistingDirData(dirClient, childPath, foundChildData);
                } // else ignore already found file NodeData
            }
        } else {
            for(val foundChildData: foundChildDatas.values()) {
                val childName = foundChildData.name;
                val childPath = destPath.toChild(childName);
                if (foundChildData.type == NodeData.TYPE_DIR) {
                    executorService.submit(() -> doTask_doRecurseOnExistingDirData(dirClient, childPath, foundChildData));
                    submittedTaskCount.incrementAndGet();
                } // else ignore already found file NodeData
            }
        }
    }
    
    private void doTask_doRecurseOnExistingDirData(DataLakeDirectoryAsyncClient dirClient, NodeNamesPath childPath,
            NodeData childData) {
        processingTaskCount.incrementAndGet();
        submittedTaskCount.decrementAndGet();
        try {
            super.doRecurseOnExistingDirData(dirClient, childPath, childData);
        } finally {
            processingTaskCount.decrementAndGet();
        }
    }

    /**
     * overrided to use Async api, or (blocking api + ) ExecutorService
     */
    @Override
    protected void loopRecurseOnMissingDirChildItems(
            DataLakeDirectoryAsyncClient dirClient,
            NodeNamesPath destPath, 
            Map<NodeName,PathItem> childDirsToRecurse) {
        if (isInterrupt()) {
            return;
        }
        if (childDirsToRecurse.size() == 1
                || (processingTaskCount.get() + 1 >= maxParallelProcessingTaskCount)
                ) {
            // sequential
            for(val e: childDirsToRecurse.entrySet()) {
                if (isInterrupt()) {
                    return;
                }
                val childName = e.getKey();
                val childPathItem = e.getValue();
                val childPath = destPath.toChild(childName);
                val childDirClient = dirClient.getSubdirectoryAsyncClient(childName.toText());
                
                putChildDir_recurseOnMissingDirChildItems(childPathItem, childPath, childDirClient);
            }
        } else {
            // parallel!! ... TOCHANGE handle first, queue remaining subList??
            for(val e: childDirsToRecurse.entrySet()) {
                val childName = e.getKey();
                val childPathItem = e.getValue();
                val childPath = destPath.toChild(childName);
                val childDirClient = dirClient.getSubdirectoryAsyncClient(childName.toText());
                
                executorService.submit(() -> doTask_putChildDir_recurseOnMissingDirChildItems(childPathItem, childPath, childDirClient));

                submittedTaskCount.incrementAndGet();
                totalSubmittedTaskCount.incr(0, prefix -> log.info(prefix + " " + childPath));
            }
        }
    }

    protected void doTask_putChildDir_recurseOnMissingDirChildItems(
            PathItem pathItem,
            NodeNamesPath destPath,
            DataLakeDirectoryAsyncClient dirClient) {
        if (isInterrupt()) {
            submittedTaskCount.decrementAndGet();
            return;
        }
        val startTime = System.currentTimeMillis();
        
        putChildDir_recurseOnMissingDirChildItems(pathItem, destPath, dirClient);

        val millis = System.currentTimeMillis() - startTime;
        totalProcessedTaskCount.incr(millis, prefix -> log.info(prefix + " " + destPath));
        
        submittedTaskCount.decrementAndGet();
    }
    
    /**
     * overrided to use Async api
     */
    @Override
    protected void putChildDir_recurseOnMissingDirChildItems(
            PathItem pathItem,
            NodeNamesPath destPath,
            DataLakeDirectoryAsyncClient dirClient
            ) {
        val name = destPath.lastNameOrEmpty();
        if (!pathItem.isDirectory()) {
            // should not occur?
            val nodeData = filePathItemToNodeData(pathItem, name);
            destTree.put(destPath, nodeData);
            return;
        }
    
        processingTaskCount.incrementAndGet(); // decremented in onComplete()
        val startAsyncTime = System.currentTimeMillis();
        val dirListingState = new AsyncRecurseDirListingState(startAsyncTime, pathItem, destPath, dirClient);
        
        // ** ASYNC az query listPath **
        PagedFlux<PathItem> asyncChildPathItems = dirClient.listPaths();

        val asyncMillis = System.currentTimeMillis() - startAsyncTime;
        azQueryAsyncListPathCounter.incr(asyncMillis, prefix -> log.info(prefix + " " + destPath));
        
        asyncChildPathItems.subscribe(
                (PathItem childPathItem) -> dirListingState.onChildPathItem(childPathItem),
                (Throwable error) -> dirListingState.onError(error),
                () -> dirListingState.onComplete()
                );
    }
    
    protected class AsyncRecurseDirListingState {
        private final long startTime;
        private final PathItem pathItem;
        private final NodeNamesPath destPath;
        private final DataLakeDirectoryAsyncClient dirClient;
        
        List<NodeName> childNames = new ArrayList<NodeName>();
        Map<NodeName,NodeData> foundChildDatas = new HashMap<>();
        Map<NodeName,PathItem> notFoundChildPathItems = new HashMap<>();
        Throwable errorOccured;

        public AsyncRecurseDirListingState(long startTime, PathItem pathItem, NodeNamesPath destPath, DataLakeDirectoryAsyncClient dirClient) {
            this.startTime = startTime;
            this.pathItem = pathItem;
            this.destPath = destPath;
            this.dirClient = dirClient;
        }

        // callback for 1 child item
        public void onChildPathItem(PathItem childPathItem) {
            val childName = AzDatalakeStoreUtils.pathItemToChildName(childPathItem, nameEncoder);
            childNames.add(childName);
            
            // TOCHECK
            val childPath = destPath.toChild(childName);
            val found = destTree.get(childPath);
            if (found != null) {
                return; // should not occur.. already present?
            }
            
            if (childPathItem.isDirectory()) {
                notFoundChildPathItems.put(childName, childPathItem);
                // enqueue for recurse
            } else {
                // file, immediate convert
                val childNodeData = filePathItemToNodeData(childPathItem, childName);
                foundChildDatas.put(childName, childNodeData);
            }
        }
        
        // callback for error
        public void onError(Throwable error) {
            // callback for error?? 
            this.errorOccured = error;
            processingTaskCount.decrementAndGet();
        }
        
        // callback for listing complete
        public void onComplete() {
            long millis = System.currentTimeMillis() - startTime;
            azQueryListPathCounter.incr(millis, prefix -> log.info(prefix + " " + destPath));
            
            if (this.errorOccured != null) {
                processingTaskCount.decrementAndGet();
                throw new RuntimeException(this.errorOccured); 
            }
            // complete dir listing: build Dir NodeData
            val name = destPath.lastNameOrEmpty();
            val sortedChildNames = ImmutableSet.copyOf(new TreeSet<>(childNames));
            val nodeData = dirPathItemToNodeData(pathItem, name, sortedChildNames);
            
            // put directory
            // also immediate put child Files .. ( use 1 call optims for data + immediate child )
            destTree.putWithChild(destPath, nodeData, foundChildDatas.values());
            
            
            if (isInterrupt()) {
                processingTaskCount.decrementAndGet();
                return;
            }
            
            // *** recurse on child sub-dirs ***
            loopRecurseOnMissingDirChildItems(dirClient, destPath, notFoundChildPathItems);
            processingTaskCount.decrementAndGet();
            
        }
    }
}
