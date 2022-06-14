package fr.an.attrtreestore.azure;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.util.LoggingCounter;
import fr.an.attrtreestore.util.LoggingCounter.LoggingCounterParams;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParallelAzRecursiveListingToTree extends AzRecursiveListingToTree {

    private ExecutorService executorService;
    
    @Getter
    private final AtomicInteger submittedTaskCount = new AtomicInteger(0);
    
    @Getter
    protected LoggingCounter totalSubmittedTaskCount = new LoggingCounter("submitted missing PathItem task", 
            new LoggingCounterParams(1_000_000, 10_000)); 
    @Getter
    protected LoggingCounter totalProcessedTaskCount = new LoggingCounter("processed missing PathItem task",
            new LoggingCounterParams(1_000_000, 10_000)); 

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
    
}
