package fr.an.attrtreestore.azure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

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
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public abstract class AzRecursiveListingToTree {

	private final ExecutorService executorService;
	private final NodeNameEncoder nameEncoder;
	
	private final DataLakeDirectoryClient srcBaseDir;
	// private final String srcBaseSubDirectoryPath; 
	
	private final IWriteTreeData destTree;
	private final NodeNamesPath destBasePath;
	
	private int maxRetry = 5;
	
	// ------------------------------------------------------------------------

	
	// ------------------------------------------------------------------------
	
	public void executeListing(
			) {
		// String azDirPath = srcBaseDir.getDirectoryName();
		NodeData nodeData = destTree.get(destBasePath);
		
		// check if already loaded (TOADD handle expired time)
		List<PathItem> optChildPathItems;
		if (nodeData == null) {
			PathProperties pathProps = srcBaseDir.getProperties();
			if (! pathProps.isDirectory()) {
				return; // should not occur
			}		
			// ** az query listPaths **
			val pathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(srcBaseDir, maxRetry);
			
			val name = destBasePath.lastNameOrEmpty();
			nodeData = dirPropsAndPathItemsToNodeData(name, pathProps, pathItems);
			destTree.put(destBasePath, nodeData);

			optChildPathItems = pathItems;
		} else {
			optChildPathItems = null;
		}
		
		recursiveDirListing(srcBaseDir, destBasePath, nodeData, optChildPathItems);
	}
	
	protected void recursiveDirListing(
			DataLakeDirectoryClient dirClient,
			NodeNamesPath destPath,
			NodeData nodeData,
			List<PathItem> optChildPathItems // may be null, will re-query
			) {
		List<PathItem> childPathItemsToRecurse = null;
			
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
		
		if (notFoundChildNames.isEmpty())  {
			// all child found in tree, can recurse without az query
			for(val foundChildData: foundChildDatas) {
				val childName = foundChildData.name;
				val childPath = destPath.toChild(childName);
				if (foundChildData.type == NodeData.TYPE_DIR) {
					val childDirClient = dirClient.getSubdirectoryClient(childName.toText());
					// *** recurse ***
					recursiveDirListing(childDirClient, childPath, foundChildData, null);
				} // else ignore already found file NodeData
			}
		} else { // need az query getProperties() or listPaths()
			// ** az query listPaths **
			val childPathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(dirClient, maxRetry);
			
			childPathItemsToRecurse = new ArrayList<>(notFoundChildNames.size());
			for(val childPathItem: childPathItems) {
				val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
				val childName = nameEncoder.encode(childFilename);
				if (! notFoundChildNames.contains(childName)) {
					childPathItemsToRecurse.add(childPathItem);
				}
			}
		}
		
		if (childPathItemsToRecurse != null && !childPathItemsToRecurse.isEmpty()) {
			val parentDirClient = dirClient;  
			for(val childPathItem: childPathItemsToRecurse) {
				val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
				val childName = nameEncoder.encode(childFilename);
				val childPath = destPath.toChild(childName);
				
				putPathItem_recurseOnMissingDirChildItems(childPathItem, childPath, parentDirClient);
			}
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
			recursiveDirListing(dirClient, destPath, nodeData, childPathItems);
			
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
		List<PathItem> childPathItems = AzDatalakeStoreUtils.retryableAzQueryListPaths(dirClient, maxRetry);
			
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
