package fr.an.attrtreestore.azure;

import com.azure.storage.file.datalake.DataLakeDirectoryAsyncClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.util.LoggingCounter;
import fr.an.attrtreestore.util.LoggingCounter.LoggingCounterParams;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class AzAsyncRecursiveListingToTree {

    protected final NodeNameEncoder nameEncoder;
	
	protected final IWriteTreeData destTree;
	
	protected int maxRetryAzQueryListPath = 5;
	
	protected final AtomicBoolean interrupt = new AtomicBoolean(false);
	
	@Getter
	protected LoggingCounter azQueryListPathCounter = new LoggingCounter("az query dir.listPaths",
            new LoggingCounterParams(1_000_000, 10_000)); 
	
	// ------------------------------------------------------------------------

	public AzAsyncRecursiveListingToTree(NodeNameEncoder nameEncoder, IWriteTreeData destTree) {
		this.nameEncoder = nameEncoder;
		this.destTree = destTree;
	}
	
	// ------------------------------------------------------------------------
	
    public void interrupt() {
        this.interrupt.set(true);
    }

    public boolean isInterrupt() {
        return this.interrupt.get();
    }

	public void executeListing(
			DataLakeDirectoryAsyncClient srcBaseDir,
			NodeNamesPath destBasePath			
			) {
	    log.info("executeListing...");
	    val startTime = System.currentTimeMillis();
	    
	    interrupt.set(false);

	    val foundChildDatas = new HashMap<NodeName,NodeData>();
	    val notFoundChildNames = new ArrayList<NodeName>();
	    
		NodeData nodeData = destTree.getWithChild(destBasePath, foundChildDatas, notFoundChildNames);

		Map<NodeName,PathItem> notFoundChildPathItems;   

		// check if already loaded (TOADD handle expired time)
		if (nodeData == null || nodeData.childNames.isEmpty()) {
			PathProperties pathProps = srcBaseDir.getProperties().block();
			if (! pathProps.isDirectory()) {
				return; // should not occur
			}		
			// ** az query listPaths **
            @SuppressWarnings("deprecation")
			val childPathItems = AzDatalakeStoreUtils.blocking_retryableAzQueryListPaths(srcBaseDir, azQueryListPathCounter, maxRetryAzQueryListPath);
			
			val childExtract = childPathItemsExtractFor(childPathItems);
			val name = destBasePath.lastNameOrEmpty();
			nodeData = dirPropsAndPathItemsToNodeData(name, pathProps, childPathItems);
			destTree.putWithChild(destBasePath, nodeData, childExtract.foundChildDatas.values());

			notFoundChildPathItems = childExtract.notFoundChildPathItems;
		} else {
		    if (notFoundChildNames.isEmpty()) {
		        notFoundChildPathItems = null; // TOCHECK?
		    } else {
		        // ** az query listPaths **
		        @SuppressWarnings("deprecation")
                val childPathItems = AzDatalakeStoreUtils.blocking_retryableAzQueryListPaths(srcBaseDir, azQueryListPathCounter, maxRetryAzQueryListPath);

		        val childExtract = childPathItemsExtractFor(childPathItems);
	            notFoundChildPathItems = new HashMap<NodeName,PathItem>();
	            for(val childName: notFoundChildNames) {
	                PathItem pathItem = childExtract.notFoundChildPathItems.get(childName);
	                val childPath = destBasePath.toChild(childName);
	                if (pathItem != null) {
	                    if (pathItem.isDirectory()) {
	                        notFoundChildPathItems.put(childName, pathItem);
	                    } else {
	                        val childNodeData = filePathItemToNodeData(pathItem, childName);
	                        foundChildDatas.put(childName, childNodeData);
	                        destTree.put(childPath, childNodeData);
	                        notFoundChildNames.remove(childName);
	                    }
	                } else {
	                    destTree.remove(childPath);
	                }
	            }
		    }
		}
		
        // TOCHECK recurse handle only missing Dirs, not Files
        
		recurseOnExistingDirData(srcBaseDir, destBasePath, nodeData, foundChildDatas, notFoundChildNames);

		val millis = System.currentTimeMillis() - startTime;
		log.info(".. done executeListing, took " + millis + " ms");
	}
		
	protected void recurseOnExistingDirData(
			DataLakeDirectoryAsyncClient dirClient, // useless here .. cf switching to recurseOnMissingDirItems
			NodeNamesPath destPath,
			NodeData nodeData,
			Map<NodeName,NodeData> foundChildDatas, // cf getWithChild()
			List<NodeName> notFoundChildNames
			) {
	    if (isInterrupt()) {
	        return;
	    }
			
		val childNames = nodeData.childNames;
		if (childNames == null || childNames.isEmpty()) {
			return;
		}
		
		if (! foundChildDatas.isEmpty()) {
			// all child found in tree, can recurse without az query
			loopRecurseOnExistingDirDatas(dirClient, destPath, foundChildDatas);
	    }

		if (! notFoundChildNames.isEmpty()) {
	        // ** az query listPaths **
		    // blocking api just for switching existing-dir -> missing-dir... then recurse on missing sub-dirs
            @SuppressWarnings("deprecation")
	        val childPathItems = AzDatalakeStoreUtils.blocking_retryableAzQueryListPaths(dirClient, azQueryListPathCounter, maxRetryAzQueryListPath);
		    
	        val childExtract = childPathItemsExtractFor(childPathItems);
	        if (childExtract.foundChildDatas.isEmpty()) {
	            // immediate add (file) child items
	            destTree.putChildList(destPath, childExtract.foundChildDatas.values());
	        }
		    // now recurse on missing child sub-dir
	        
			val childSubDirsToRecurse = new HashMap<NodeName,PathItem>();
			for(val childPathItem: childPathItems) {
				val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
				val childName = nameEncoder.encode(childFilename);
				if (notFoundChildNames.contains(childName)) {
				    if (childPathItem.isDirectory()) {
				        childSubDirsToRecurse.put(childName, childPathItem);
				    } else {
				        // should not occur, cf above childExtract.foundChildDatas
				        // immediate hande File.. no recurse
				        val childNodeData = filePathItemToNodeData(childPathItem, childName);
				        val childPath = destPath.toChild(childName);
			            destTree.put(childPath, childNodeData);
				    }
				}
			}
			loopRecurseOnMissingDirChildItems(dirClient, destPath, childSubDirsToRecurse);
		}
	}

	/**
	 * may override to use ExecutorService
	 */
    protected void loopRecurseOnExistingDirDatas(
            DataLakeDirectoryAsyncClient dirClient, 
            NodeNamesPath destPath,
            Map<NodeName, NodeData> foundChildDatas) {
        for(val foundChildData: foundChildDatas.values()) {
        	val childName = foundChildData.name;
        	val childPath = destPath.toChild(childName);
        	if (foundChildData.type == NodeData.TYPE_DIR) {
        	    doRecurseOnExistingDirData(dirClient, childPath, foundChildData);
        	} // else ignore already found file NodeData
        }
    }

    protected void doRecurseOnExistingDirData(
            DataLakeDirectoryAsyncClient dirClient,
            NodeNamesPath childPath,
            NodeData childData
            ) {
        // re-query for sub-child
        val foundSubChildDatas = new HashMap<NodeName,NodeData>(); 
        val notFoundSubChildNames = new ArrayList<NodeName>();

        val childData2 = destTree.getWithChild(childPath, foundSubChildDatas, notFoundSubChildNames);
        
        if (childData2 != childData) {
            // should not occur
        }
        
        val childDirClient = dirClient.getSubdirectoryAsyncClient(childData.name.toText());

        // *** recurse ***
        recurseOnExistingDirData(childDirClient, childPath, 
                childData, foundSubChildDatas, notFoundSubChildNames);
    }

	/**
	 * may override to use Async api, or (blocking api + ) ExecutorService
	 */
	protected void loopRecurseOnMissingDirChildItems(
			DataLakeDirectoryAsyncClient dirClient,
			NodeNamesPath destPath, 
			Map<NodeName,PathItem> childDirsToRecurse) {
	    if (isInterrupt()) {
		    return;
		}
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
	}
	
	protected void putChildDir_recurseOnMissingDirChildItems(
			PathItem pathItem,
			NodeNamesPath destPath,
			DataLakeDirectoryAsyncClient dirClient
			) {
		val name = destPath.lastNameOrEmpty();

		// ** az query listPath **
		val nodeDataAndPathItems = dirPathItemToNodeData(pathItem, name, dirClient);
		
		NodeData nodeData = nodeDataAndPathItems.nodeData;
		
		val childExtract = childPathItemsExtractFor(nodeDataAndPathItems.childPathItems);
		
		// put directory + also immediate child Files .. ( use 1 call optims for data + immediate child )
		destTree.putWithChild(destPath, nodeData, childExtract.foundChildDatas.values());
		
		if (isInterrupt()) {
		    return;
		}
		
		val childDirsToRecurse = childExtract.notFoundChildPathItems;
		
		// *** recurse on child sub-dirs ***
		loopRecurseOnMissingDirChildItems(dirClient, destPath, childDirsToRecurse);
	}

	// utility for scanning
	// ------------------------------------------------------------------------
	
    protected ChildItemsExtract childPathItemsExtractFor(List<PathItem> childPathItems) {
        val res = new ChildItemsExtract();
        res.addPathItems(childPathItems);
        return res;
    }

    protected class ChildItemsExtract {
        Map<NodeName,NodeData> foundChildDatas = new HashMap<>();
        Map<NodeName,PathItem> notFoundChildPathItems = new HashMap<>();
        List<NodeName> childNames = new ArrayList<>();
        
        public void addPathItem(PathItem childPathItem) {
            val childName = AzDatalakeStoreUtils.pathItemToChildName(childPathItem, nameEncoder);
            childNames.add(childName);
            if (childPathItem.isDirectory()) {
                notFoundChildPathItems.put(childName, childPathItem);
            } else {
                val childNodeData = filePathItemToNodeData(childPathItem, childName);
                foundChildDatas.put(childName, childNodeData);
            }
        }

        public void addPathItems(List<PathItem> childPathItems) {
            for(val childPathItem: childPathItems) {
                addPathItem(childPathItem);
            }
        }
    }
	
	/** convert file PathItem to NodeData 
	 * notice: creationTime field not set 
	 */
	protected NodeData filePathItemToNodeData(PathItem pathItem, NodeName name) {
		assert ! pathItem.isDirectory();
		int mask = 0;
		val attrs = ImmutableMap.<String, NodeAttr>of(
				// TOADD?
				);  
		long externalCreationTime = 0L; // NOT available in PathItem!! ... incomplete NodeData 
		long externalLastModifiedTime = AzDatalakeStoreUtils.toMillis(pathItem.getLastModified()); 
		
		val childNames = ImmutableSet.<NodeName>of();
		long externalLength = pathItem.getContentLength();
		
		return new NodeData(name, NodeData.TYPE_FILE, mask, 
				childNames, attrs, //  
				externalCreationTime, externalLastModifiedTime, externalLength, // 
				0L, 0 // lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount
				);
	}
	
	@AllArgsConstructor
	protected static class NodeDataAndChildPathItems {
		NodeData nodeData;
		List<PathItem> childPathItems;
	}
	
	/** convert dir PathItem to NodeData, doing query az listPath 
	 * notice: creationTime field not set 
	 */
	@Deprecated // async->blocking api!
	protected NodeDataAndChildPathItems dirPathItemToNodeData(PathItem pathItem, NodeName name,
			DataLakeDirectoryAsyncClient dirClient) {
		assert pathItem.isDirectory();
		int mask = 0;
		
		val attrs = ImmutableMap.<String, NodeAttr>of(
				// TOADD?
				);  
		long externalCreationTime = 0L; // NOT available in PathItem!! ... incomplete NodeData 
		long externalLastModifiedTime = AzDatalakeStoreUtils.toMillis(pathItem.getLastModified()); 
		
		// val childDirClient = parentDirClient.getSubdirectoryClient(name.toText());

		// *** az query listPaths ***
		List<PathItem> childPathItems = AzDatalakeStoreUtils.blocking_retryableAzQueryListPaths(dirClient, azQueryListPathCounter, maxRetryAzQueryListPath);
			
		val childNames = pathItemsToChildNames(childPathItems);
		
		val nodeData = new NodeData(name, NodeData.TYPE_DIR, mask, 
				childNames, attrs, //  
				externalCreationTime, externalLastModifiedTime, 0L, // 
				0L, 0 // lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount
				);
		return new NodeDataAndChildPathItems(nodeData, childPathItems);
	}

	protected static NodeData dirPathItemToNodeData(PathItem pathItem, NodeName name,
	        ImmutableSet<NodeName> childNames) {
	    assert pathItem.isDirectory();
	    int mask = 0;

	    val attrs = ImmutableMap.<String, NodeAttr>of(
	            // TOADD?
	            );  
	    long externalCreationTime = 0L; // NOT available in PathItem!! ... incomplete NodeData 
	    long externalLastModifiedTime = AzDatalakeStoreUtils.toMillis(pathItem.getLastModified()); 

	    val nodeData = new NodeData(name, NodeData.TYPE_DIR, mask, 
	            childNames, attrs, //  
	            externalCreationTime, externalLastModifiedTime, 0L, // 
	            0L, 0 // lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount
	            );
	    return nodeData;
	}

	   
	/** convert List<PathItem> to ImmutableSet<NodeName> childNames */
	protected ImmutableSet<NodeName> pathItemsToChildNames(List<PathItem> pathItems) {
		val sortedChildNames = new TreeSet<NodeName>();
		for(val childPathItem: pathItems) {
			val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
			val childName = nameEncoder.encode(childFilename);
			sortedChildNames.add(childName);
		}
		return ImmutableSet.copyOf(sortedChildNames);
	}
	
	/** convert directory PathProps+PathItems to NodeData */
	protected NodeData dirPropsAndPathItemsToNodeData(NodeName name, PathProperties props, List<PathItem> childPathItems) {
		assert props.isDirectory();
		int mask = 0;
		
		val childNames = pathItemsToChildNames(childPathItems);
		val attrs = ImmutableMap.<String, NodeAttr>of(
				// TOADD?
				);  
		long externalCreationTime = AzDatalakeStoreUtils.toMillis(props.getCreationTime()); 
		long externalLastModifiedTime = AzDatalakeStoreUtils.toMillis(props.getLastModified()); 
		
		return new NodeData(name, NodeData.TYPE_DIR, mask, 
				childNames, attrs, //  
				externalCreationTime, externalLastModifiedTime, 0L, // 
				0L, 0 // lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount
				);
	}

}
