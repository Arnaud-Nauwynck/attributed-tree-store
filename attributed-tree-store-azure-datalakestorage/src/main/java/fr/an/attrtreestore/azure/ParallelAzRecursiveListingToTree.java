package fr.an.attrtreestore.azure;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.models.PathItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.util.LoggingCounter;
import fr.an.attrtreestore.util.LoggingCounter.LoggingCounterParams;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Deprecated // cf AsyncParallelAzRecursiveListingToTree
public class ParallelAzRecursiveListingToTree extends AzRecursiveListingToTree {

    private ExecutorService executorService;
    
    @Getter
    private final AtomicInteger submittedTaskCount = new AtomicInteger(0);
    
    @Getter
    protected LoggingCounter totalSubmittedTaskCount = new LoggingCounter("submitted missing PathItem task", 
            new LoggingCounterParams(1_000_000, 600_000)); 
    @Getter
    protected LoggingCounter totalProcessedTaskCount = new LoggingCounter("processed missing PathItem task",
            new LoggingCounterParams(1_000_000, 600_000)); 

    // --------------------------------------------------------------------------------------------

    public ParallelAzRecursiveListingToTree(NodeNameEncoder nameEncoder, IWriteTreeData destTree, //
            ExecutorService executorService
            ) {
        super(nameEncoder, destTree);
        this.executorService = executorService;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void executeListing(DataLakeDirectoryClient srcBaseDir, NodeNamesPath destBasePath) {
        long startTime = System.currentTimeMillis();
        int periodicDelayMillis = 10_000;
        log.info("... start executeListing (async tasks) " 
                + srcBaseDir.getDirectoryUrl()
                + " + periodic monitor progress every " + (periodicDelayMillis/1000) + "s");

        // => async launch tasks...
        super.executeListing(srcBaseDir, destBasePath);
        
        try {
            Thread.sleep(periodicDelayMillis);
        } catch (InterruptedException e) {
        }
        
        // wait until tasks are finished
        for(;;) {
            if (this.interrupt) {
                log.info("interrupted executeListing");
                return;
            }
            int count = submittedTaskCount.get();
            if (count == 0) {
                break; // finished!
            }
            if (count > 1) {
                log.info("curr pending dirListing tasks count: " + count);
                try {
                    Thread.sleep(periodicDelayMillis);
                } catch (InterruptedException e) {
                }
            }
        }
        
        long millis = System.currentTimeMillis() - startTime;
        log.info("... Finished executeListing " 
                + srcBaseDir.getDirectoryUrl()
                + ", took " + (millis/1000) + "s");
    }
    

    @Override
    protected void putChildPathItemList_recurseOnMissingDirChildItems(
            DataLakeDirectoryClient parentDirClient,
            NodeNamesPath parentDestPath, 
            List<PathItemAndNodeName> childToRecurse) {
        if (this.interrupt) {
            return;
        }
        if (childToRecurse.size() <= 2) {
            for(val child: childToRecurse) { 
                val childPathItem = child.pathItem;
                val childPath = parentDestPath.toChild(child.name);
                putPathItem_recurseOnMissingDirChildItems(childPathItem, childPath, parentDirClient);
            }
            return;
        }
        // TOCHECK ... handle first, queue remaining subList??
        for(val child: childToRecurse) {
            try {
                executorService.submit(() -> doTaskPutChildPathItem_recurseOnMissingDirChildItems(parentDirClient, parentDestPath, child));
                submittedTaskCount.incrementAndGet();
                totalSubmittedTaskCount.incr(0, prefix -> log.info(prefix + " " + parentDestPath + "/" + child.name));
            } catch(RejectedExecutionException ex) {
                log.error("Failed to submit task?", ex);
            }
        }
    }

    protected void doTaskPutChildPathItem_recurseOnMissingDirChildItems(DataLakeDirectoryClient parentDirClient,
            NodeNamesPath parentDestPath,
            PathItemAndNodeName child) {
        if (this.interrupt) {
            submittedTaskCount.decrementAndGet();
            return;
        }
        
        val startTime = System.currentTimeMillis();
        
        val childPathItem = child.pathItem;
        val childPath = parentDestPath.toChild(child.name);
        putPathItem_recurseOnMissingDirChildItems(childPathItem, childPath, parentDirClient);


        val millis = System.currentTimeMillis() - startTime;
        totalProcessedTaskCount.incr(millis, prefix -> log.info(prefix + " " + childPath));
        
        submittedTaskCount.decrementAndGet();
    }
    
    /**
     * override to use Async api
     */
    @Override
    protected void putPathItem_recurseOnMissingDirChildItems(
            PathItem pathItem,
            NodeNamesPath destPath,
            DataLakeDirectoryClient parentDirClient
            ) {
        val name = destPath.lastNameOrEmpty();
        if (!pathItem.isDirectory()) {
            // should not occur?
            val nodeData = filePathItemToNodeData(pathItem, name);
            destTree.put(destPath, nodeData);
            return;
        }
        
    
        val dirClient = parentDirClient.getSubdirectoryClient(name.toText());
        
        // ** ASYNC az query listPath **
        val nodeDataAndPathItems = dirPathItemToNodeData(pathItem, name, dirClient);
        
        NodeData nodeData = nodeDataAndPathItems.nodeData;
        List<PathItem> childPathItems = nodeDataAndPathItems.childPathItems;
        
        Map<NodeName,NodeData> foundChildDatas = new HashMap<>();
        List<NodeName> notFoundChildNames = new ArrayList<>();
        List<NodeData> childNodeDatas = new ArrayList<>();
        for(val childPathItem: childPathItems) {
            val childName = AzDatalakeStoreUtils.pathItemToChildName(childPathItem, nameEncoder);
            if (childPathItem.isDirectory()) {
                notFoundChildNames.add(childName);
            } else {
                val childNodeData = filePathItemToNodeData(childPathItem, childName);
                childNodeDatas.add(childNodeData);
                foundChildDatas.put(childName, childNodeData);
            }
        }
        
        // put directory
        // also immediate put child Files .. ( use 1 call optims for data + immediate child )
        destTree.putWithChild(destPath, nodeData, childNodeDatas);
        
        if (interrupt) {
            return;
        }
        
        // *** recurse on child sub-dirs ***
        recurseOnMissingDirChildItems(dirClient, destPath, nodeData, foundChildDatas, notFoundChildNames, childPathItems);
            
    }
    
}
