package fr.an.attrtreestore.azure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
// @Deprecated // cf AzAsyncRecursiveListingToTree
public class AzRecursiveListingToTree {

    protected final NodeNameEncoder nameEncoder;
	
	protected final IWriteTreeData destTree;
	
	protected int maxRetryAzQueryListPath = 5;
	
	protected boolean interrupt = false;
	
	@Getter
	protected LoggingCounter azQueryListPathCounter = new LoggingCounter("az query dir.listPaths",
            new LoggingCounterParams(1_000_000, 10_000)); 
	
	// ------------------------------------------------------------------------

	public AzRecursiveListingToTree(NodeNameEncoder nameEncoder, IWriteTreeData destTree) {
		this.nameEncoder = nameEncoder;
		this.destTree = destTree;
	}
	
	// ------------------------------------------------------------------------
	
    public void interrupt() {
        this.interrupt = true;
    }

	public void executeListing(
			DataLakeDirectoryClient srcBaseDir,
			NodeNamesPath destBasePath			
			) {
	    log.info("executeListing...");
	    val startTime = System.currentTimeMillis();
	    
	    this.interrupt = false;

	    val foundChildDatas = new HashMap<NodeName,NodeData>();
	    val notFoundChildNames = new ArrayList<NodeName>();   

		NodeData nodeData = destTree.getWithChild(destBasePath, foundChildDatas, notFoundChildNames);
		
		// check if already loaded (TOADD handle expired time)
		List<PathItem> optChildPathItems;
		if (nodeData == null || nodeData.childNames.isEmpty()) {
			PathProperties pathProps = srcBaseDir.getProperties();
			if (! pathProps.isDirectory()) {
				return; // should not occur
			}		
			// ** az query listPaths **
			val pathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(srcBaseDir, azQueryListPathCounter, maxRetryAzQueryListPath);
			
			val name = destBasePath.lastNameOrEmpty();
			nodeData = dirPropsAndPathItemsToNodeData(name, pathProps, pathItems);
			destTree.put(destBasePath, nodeData);

			notFoundChildNames.addAll(AzDatalakeStoreUtils.pathItemsToChildNames(pathItems, nameEncoder));
			
			optChildPathItems = pathItems;
		} else {
			optChildPathItems = null;
		}
		
        // TOCHECK recurse handle only missing Dirs, not Files
		
        
		recurseOnMissingDirChildItems(srcBaseDir, destBasePath, nodeData, foundChildDatas, notFoundChildNames, optChildPathItems);

		val millis = System.currentTimeMillis() - startTime;
		log.info(".. done executeListing, took " + millis + " ms");
	}
		
	protected void recurseOnMissingDirChildItems(
			DataLakeDirectoryClient dirClient,
			NodeNamesPath destPath,
			NodeData nodeData, // ensure alreay put in tree, and obtained by getWithChild()
			Map<NodeName,NodeData> foundChildDatas, // cf getWithChild()
	        List<NodeName> notFoundChildNames,	 // cf getWithChild()	
			List<PathItem> optChildPathItems // may be null, will re-query
			) {
	    if (this.interrupt) {
	        return;
	    }
			
		// recurse with unknown child types, only childNames
		val childNames = nodeData.childNames;
		if (childNames == null || childNames.isEmpty()) {
			return;
		}
		
		if (! foundChildDatas.isEmpty()) {
			// all child found in tree, can recurse without az query
			for(val foundChildData: foundChildDatas.values()) {
				val childName = foundChildData.name;
				val childPath = destPath.toChild(childName);
				if (foundChildData.type == NodeData.TYPE_DIR) {
				    // re-query for sub-child
				    val foundSubChildDatas = new HashMap<NodeName,NodeData>(); 
				    val notFoundSubChildNames = new ArrayList<NodeName>();   
				    val childData = destTree.getWithChild(childPath, foundSubChildDatas, notFoundSubChildNames);
				    if (childData != foundChildData) {
				        // should not occur
				    }
				    
					val childDirClient = dirClient.getSubdirectoryClient(childName.toText());
					// *** recurse ***
					recurseOnMissingDirChildItems(childDirClient, childPath, 
					        childData, foundSubChildDatas, notFoundSubChildNames,
					        null);
				} // else ignore already found file NodeData
			}
	    }

		if (! notFoundChildNames.isEmpty()) {
			// need az query getProperties() or listPaths()
		    
		    List<PathItem> childPathItems;
		    if (optChildPathItems != null) {
		        childPathItems = optChildPathItems;
		    } else {
		        // ** az query listPaths **
		        childPathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(dirClient, azQueryListPathCounter, maxRetryAzQueryListPath);
		    
		        // TODO immediate put Dir ???
		        // instead of after child
		    }
		    
			val childSubDirToRecurse = new ArrayList<PathItemAndNodeName>(notFoundChildNames.size());
			for(val childPathItem: childPathItems) {
				val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
				val childName = nameEncoder.encode(childFilename);
				if (notFoundChildNames.contains(childName)) {
				    if (childPathItem.isDirectory()) {
				        childSubDirToRecurse.add(new PathItemAndNodeName(childPathItem, childName));
				    } else {
				        // immediate hande File.. no recurse
				        val childNodeData = filePathItemToNodeData(childPathItem, childName);
				        val childPath = destPath.toChild(childName);
			            destTree.put(childPath, childNodeData);
				    }
				}
			}
			putChildPathItemList_recurseOnMissingDirChildItems(dirClient, destPath, childSubDirToRecurse);
		}
	}

   @AllArgsConstructor
    protected static class PathItemAndNodeName {
        PathItem pathItem;
        NodeName name;
    }

	/**
	 * may override to use Async api, or (blocking api + ) ExecutorService
	 */
	protected void putChildPathItemList_recurseOnMissingDirChildItems(
			DataLakeDirectoryClient parentDirClient,
			NodeNamesPath parentDestPath, 
			List<PathItemAndNodeName> childToRecurse) {
		if (this.interrupt) {
		    return;
		}
	    for(val child: childToRecurse) {
	        if (this.interrupt) {
	            return;
	        }
	        
	        val childPathItem = child.pathItem;
	        val childPath = parentDestPath.toChild(child.name);
			putPathItem_recurseOnMissingDirChildItems(childPathItem, childPath, parentDirClient);
		}
	}
	
	protected void putPathItem_recurseOnMissingDirChildItems(
			PathItem pathItem,
			NodeNamesPath destPath,
			DataLakeDirectoryClient parentDirClient
			) {
		val name = destPath.lastNameOrEmpty();
		if (pathItem.isDirectory()) {
			val dirClient = parentDirClient.getSubdirectoryClient(name.toText());
			// ** az query listPath **
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
			
		} else { // file
			val nodeData = filePathItemToNodeData(pathItem, name);
			destTree.put(destPath, nodeData);
		}
	}

	// utility for scanning
	// ------------------------------------------------------------------------
	
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
	protected NodeDataAndChildPathItems dirPathItemToNodeData(PathItem pathItem, NodeName name,
			DataLakeDirectoryClient dirClient) {
		assert pathItem.isDirectory();
		int mask = 0;
		
		val attrs = ImmutableMap.<String, NodeAttr>of(
				// TOADD?
				);  
		long externalCreationTime = 0L; // NOT available in PathItem!! ... incomplete NodeData 
		long externalLastModifiedTime = AzDatalakeStoreUtils.toMillis(pathItem.getLastModified()); 
		
		// val childDirClient = parentDirClient.getSubdirectoryClient(name.toText());

		// *** az query listPaths ***
		List<PathItem> childPathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(dirClient, azQueryListPathCounter, maxRetryAzQueryListPath);
			
		val childNames = pathItemsToChildNames(childPathItems);
		
		val nodeData = new NodeData(name, NodeData.TYPE_DIR, mask, 
				childNames, attrs, //  
				externalCreationTime, externalLastModifiedTime, 0L, // 
				0L, 0 // lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount
				);
		return new NodeDataAndChildPathItems(nodeData, childPathItems);
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
