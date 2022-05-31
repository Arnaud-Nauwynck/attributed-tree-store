package fr.an.attrtreestore.storage.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.api.ReadOnlyCached_TreeNodeData;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * 
 */
public class Cached_ReadOnlyIndexedBlobStorage_TreeNodeData extends ReadOnlyCached_TreeNodeData {
	
	private final BlobStorage blobStorage;

	@Getter
	private final String fileName;

	private final IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder;

	private final CachedNodeEntry rootNode;
	
	private final long fileLen; // computed from blobStorage + fileName at init

	private int fetchBlockSize = 4096; //
	
	/**
	 * partially loaded Node... 
	 * immutable except for loading children from cache /evicting children out of memory 
	 * 
	 * ... trying to be memory efficient, using (sorted) Arrays instead of Map<> and sub objects..
	 * 
	 */
	@RequiredArgsConstructor //?
	@AllArgsConstructor
	private static class CachedNodeEntry {
		private final NodeName name;
		
		private long dataFilePos;
		// private int dataLen; // .. redundant with filePos 
		private NodeData cachedData;

		// when loaded => downcast to CachedNodeEntry
		// else => downcast to ChildEntryHandle 
		private Object[] sortedEntries;

		int findChildIndex(NodeName childName) {
			// dichotomy search child index by name
			// cf Arrays.binarySearch
			val childArray = sortedEntries; 
	        int low = 0;
	        int high = childArray.length - 1;
	        while (low <= high) {
	            int mid = (low + high) >>> 1;
	            Object midVal = childArray[mid];
	            NodeName midName = (midVal instanceof CachedNodeEntry)? ((CachedNodeEntry) midVal).name : ((NodeEntryHandle) midVal).name;
	            int cmp = midName.compareTo(childName);            
	            if (cmp < 0) {
	                low = mid + 1;
	            } else if (cmp > 0) {
	                high = mid - 1;
	            } else {
	            	return mid;
	            }
	        }
	        return -(low + 1);  // key not found.
		}
		
	}
	
	@AllArgsConstructor
	private static class NodeEntryHandle {
		final NodeName name;
		long dataFilePos;
		// private int dataLen; // redundant with filePos..
	}
	
	// ------------------------------------------------------------------------
	
	public Cached_ReadOnlyIndexedBlobStorage_TreeNodeData(BlobStorage blobStorage, String fileName,
			IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder) {
		this.blobStorage = blobStorage;
		this.fileName = fileName;
		this.indexedTreeNodeDataEncoder = indexedTreeNodeDataEncoder;
		
		this.fileLen = blobStorage.fileLen(fileName);
		
		// load at least the root node, maybe few others recursively..
		this.rootNode = null; // TODO

	}

	// implements api ReadOnlyCached_TreeNodeData
	// ------------------------------------------------------------------------
	
	@Override
	public NodeData get(NodeNamesPath path) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		CachedNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			val childIdx = currEntry.findChildIndex(pathElt);
			if (childIdx < 0) {
				return null; // Node not found 
			}
			Object childObj = currEntry.sortedEntries[childIdx];
			if (childObj == null) {
				return null;
			}
			if (childObj instanceof CachedNodeEntry) {
				currEntry = (CachedNodeEntry) childObj; 
			} else { // NodeEntryHandle
				val childEntryHandle = (NodeEntryHandle) childObj;
				// cache miss .. need to async reload entry from cache
				CachedNodeEntry childEntry = doLoadCachedNodeEntry(childEntryHandle);
				currEntry.sortedEntries[childIdx] = childEntry;
				currEntry = childEntry;
			}
		}
		NodeData res = currEntry.cachedData;
		if (res == null) {
			// cache miss on last entry on cachedData
			val currHandle = new NodeEntryHandle(currEntry.name, currEntry.dataFilePos);
			CachedNodeEntry reloadCurrEntry = doLoadCachedNodeEntry(currHandle);
			res = reloadCurrEntry.cachedData;
		}
		return null;
	} 

	// ------------------------------------------------------------------------
	
	private CachedNodeEntry doLoadCachedNodeEntry(NodeEntryHandle entryHandle) {
		CachedNodeEntry res;
		val dataFilePos = entryHandle.dataFilePos;
		int fetchDataLen = Math.min(fetchBlockSize, (int) (fileLen - dataFilePos));
		
		byte[] fetchData = blobStorage.readAt(fileName, dataFilePos, fetchDataLen);
		// TODO check read enough..

		val name = entryHandle.name;
		try (val in = new DataInputStream(new ByteArrayInputStream(fetchData))) {
			
			val dataAndChildPos = indexedTreeNodeDataEncoder.readNodeDataAndChildIndexes(in, name);
			
			val cachedData = dataAndChildPos.nodeData;
			val childDataFilePos = dataAndChildPos.childDataFilePos;
			// rebuild sortedEntries from childNames + childDataFilePos 
			val childNames = new ArrayList<NodeName>(cachedData.childNames);
			val childCount = childNames.size(); // assume sorted by NodeName!
			Object[] sortedEntries = new Object[childCount];
			for(int i = 0; i < childCount; i++) {
				val childName = childNames.get(i);
				sortedEntries[i] = new NodeEntryHandle(childName, childDataFilePos[i]);
			}
			
			res = new CachedNodeEntry(name, dataFilePos, cachedData, sortedEntries);
			
			// TOADD ... also parse recursively few other entries from fetchedData...
			
		} catch(IOException ex) {
			throw new RuntimeException("Failed to load", ex);
		}
		return res;
	}
	
//	public CompletableFuture<Node[]> asyncLoadChildArray(NodeTreeLoader ownerLoader) {
//		val childCount = sortedEntries.length;
//		val res = new Node[childCount];
//		CompletableFuture<Node[]> f = CompletableFuture.completedFuture(res);
//		// combine load child one by one... not using combineAll (may not be faster.. file should be sequentially read by child)
//		for(int i = 0; i < childCount; i++) {
//			val e = sortedEntries[i];
//			if (e instanceof Node) {
//				res[i] = (Node) e;
//			} else {
//				val eh = (NodeEntryHandle) e;
//				val finalI = i;
//				f = f.thenCompose(x -> 
//					ownerLoader.asyncLoadChild(this, eh.name, eh.filePos)
//						.thenApply(childNode -> { res[finalI] = childNode; return x; })
//						);
//			}
//		}
//		return f;
//	}

	
}
