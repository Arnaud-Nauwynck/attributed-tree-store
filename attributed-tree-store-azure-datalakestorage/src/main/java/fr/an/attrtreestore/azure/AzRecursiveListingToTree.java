package fr.an.attrtreestore.azure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
import lombok.Setter;
import lombok.val;

/**
 * 
 */
public abstract class AzRecursiveListingToTree {

	protected final IWriteTreeData destTree;
	
	protected final NodeNameEncoder nameEncoder;
	
	@Getter @Setter
	protected int maxRetryAzQueryListPath = 5;
	
	// ------------------------------------------------------------------------

	public AzRecursiveListingToTree(IWriteTreeData destTree, NodeNameEncoder nameEncoder) {
		this.destTree = destTree;
		this.nameEncoder = nameEncoder;
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
	    
		// String azDirPath = srcBaseDir.getDirectoryName();
		NodeData nodeData = destTree.get(destBasePath);
		
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

			optChildPathItems = pathItems;
		} else {
			optChildPathItems = null;
		}
		
		recurseOnMissingDirChildItems(srcBaseDir, destBasePath, nodeData, optChildPathItems);

		val millis = System.currentTimeMillis() - startTime;
		log.info(".. done executeListing, took " + millis + " ms");
	}
		
	protected void recurseOnMissingDirChildItems(
			DataLakeDirectoryClient dirClient,
			NodeNamesPath destPath,
			NodeData nodeData,
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
		List<NodeData> foundChildDatas = new ArrayList<>(childNames.size());
		Set<NodeName> notFoundChildNames = new HashSet<>();
		for(val childName: childNames) {
			val childPath = destPath.toChild(childName);
			val found = destTree.get(childPath);
			if (found != null) {
				foundChildDatas.add(found);
			} else {
				notFoundChildNames.add(childName);
			}
		}
		
		if (! foundChildDatas.isEmpty()) {
			// all child found in tree, can recurse without az query
			for(val foundChildData: foundChildDatas) {
				val childName = foundChildData.name;
				val childPath = destPath.toChild(childName);
				if (foundChildData.type == NodeData.TYPE_DIR) {
					val childDirClient = dirClient.getSubdirectoryClient(childName.toText());
					// *** recurse ***
					recurseOnMissingDirChildItems(childDirClient, childPath, foundChildData, null);
				} // else ignore already found file NodeData
			}
	    }

		if (! notFoundChildNames.isEmpty()) {
			// need az query getProperties() or listPaths()
			// ** az query listPaths **
			val childPathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(dirClient, azQueryListPathCounter, maxRetryAzQueryListPath);
			
			val childToRecurse = new ArrayList<PathItemAndNodeName>(notFoundChildNames.size());
			for(val childPathItem: childPathItems) {
				val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
				val childName = nameEncoder.encode(childFilename);
				if (notFoundChildNames.contains(childName)) {
				    if (childPathItem.isDirectory()) {
				        childToRecurse.add(new PathItemAndNodeName(childPathItem, childName));
				    } else {
				        // immediate hande.. no recurse
				        val childNodeData = filePathItemToNodeData(childPathItem, childName);
				        val childPath = destPath.toChild(childName);
			            destTree.put(childPath, childNodeData);
				    }
				}
			}
			putChildPathItemList_recurseOnMissingDirChildItems(dirClient, destPath, childToRecurse);
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
			
			destTree.put(destPath, nodeData);
			
			// *** recurse ***
			recurseOnMissingDirChildItems(dirClient, destPath, nodeData, childPathItems);
			
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
