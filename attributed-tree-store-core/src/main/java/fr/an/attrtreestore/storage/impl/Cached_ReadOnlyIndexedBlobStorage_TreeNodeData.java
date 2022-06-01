package fr.an.attrtreestore.storage.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoderOptions;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.api.ReadOnlyCached_TreeNodeData;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder.NodeDataAndChildFilePos;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class Cached_ReadOnlyIndexedBlobStorage_TreeNodeData extends ReadOnlyCached_TreeNodeData {

	private final BlobStorage blobStorage;

	@Getter
	private final String fileName;

	private final IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder;

	private final CachedNodeEntry rootNode;
	
	private final long fileLen; // computed from blobStorage + fileName at init

	private int fetchBlockSize = 32 * 1024; // 32k should be enough to load most data + childNames??
	// ( may use 128k..1M for full recursive batches, cf also preload at init)
	
	private int tryParseEntryThresholdSize = 100;
	
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
		final NodeName name;
		
		long dataFilePos;
		// private int dataLen; // .. redundant with filePos 
		NodeData cachedData;

		// when loaded => downcast to CachedNodeEntry
		// else => downcast to NodeEntryHandle 
		Object[] sortedEntries;

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
		
		// init the root node, dataFilePos fixed known in file 
		// field 'rootNode' is final, so must be set in ctor... 
		val rootName = DefaultNodeNameEncoderOptions.EMPTY_NAME;
		val rootDataFilePos = IndexedBlobStorage_TreeNodeDataEncoder.FIXED_ROOT_FILEPOS;
		this.rootNode = new CachedNodeEntry(rootName, rootDataFilePos, 
				null, // cachedData... can be null, and filePos is set to reload it 
				null // <= sortedEntries ... must be loaded in init!!
				);

		// initialize rootNode content!! (data + corresponding sortedEntries)
		val rootHandle = new NodeEntryHandle(rootName, rootDataFilePos);
		val loadedRootNode = doLoadCachedNodeEntry(rootHandle);
		synchronized(this.rootNode) {
			this.rootNode.cachedData = loadedRootNode.cachedData;
			this.rootNode.sortedEntries = loadedRootNode.sortedEntries;
		}

		// TOADD preload, maybe few others recursively..

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
		int maxReadLen = (int) (fileLen - dataFilePos);
		int fetchDataLen = Math.min(fetchBlockSize, maxReadLen);
		
		byte[] fetchData = blobStorage.readAt(fileName, dataFilePos, fetchDataLen);

		val name = entryHandle.name;
		NodeDataAndChildFilePos dataAndChildPos = null;
		
		// check read enough data
		// no dataLen written in file for skip ??... would need anyway to do 2 remote calls, 1 to read size, then 1 to read data..
		ByteArrayInputStream fetchDataStream = new ByteArrayInputStream(fetchData);
		int fetchDataStreamLen = fetchData.length;
		{
			boolean needMoreData = false;
			try (val in = new DataInputStream(fetchDataStream)) {
				dataAndChildPos = indexedTreeNodeDataEncoder.readNodeDataAndChildIndexes(in, name);
			} catch(EOFException ex) {
				// ok not enough data => need to load more (maybe with retry loop..)
				needMoreData = true;
			} catch(Exception ex) {
				throw new RuntimeException("should not occur", ex);
			}
			if (needMoreData) {
				long currFetchedPos = dataFilePos + fetchData.length;
				int currFetchBlockSize = fetchBlockSize;
				int maxRetry = 10;
				for(int retry = 0; retry < maxRetry; retry++) {
					// load more data..
					int fetchMoreDataLen = Math.min(currFetchBlockSize, (int) (fileLen - currFetchedPos));
					byte[] fetchMoreData = blobStorage.readAt(fileName, currFetchedPos, fetchMoreDataLen);
					byte[] concatData = new byte[fetchData.length + fetchMoreData.length];
					System.arraycopy(fetchData, 0, concatData, 0, fetchData.length);
					System.arraycopy(fetchMoreData, 0, concatData, fetchData.length, fetchMoreData.length);
					fetchData = concatData;
					fetchMoreData = concatData = null;
					fetchDataStream = new ByteArrayInputStream(fetchData);
					fetchDataStreamLen = fetchData.length;
					
					// retry parse fetchedData
					try (val in = new DataInputStream(fetchDataStream)) {
						dataAndChildPos = indexedTreeNodeDataEncoder.readNodeDataAndChildIndexes(in, name);
					} catch(EOFException ex) {
						// ok not enough data => load more and retry..
						if (retry+1 < maxRetry) {
							// increase size and retry more
							currFetchBlockSize = currFetchBlockSize + fetchBlockSize + currFetchBlockSize/2;
						} else {
							throw new RuntimeException("EOF trying to read entryData at filePos:" + dataFilePos + " using " + fetchData.length + " bytes");
						}
					} catch(Exception ex) {
						throw new RuntimeException("should not occur", ex);
					}
				}
			}
		}
				
		res = dataAndChildPosToCachedEntry(name, dataFilePos, dataAndChildPos);
		
		long remainFetchedLen = fetchDataStream.available();
		if (remainFetchedLen > tryParseEntryThresholdSize) {
			// also try parse recursively few other entries from fetchedData
			try {
				long currDataFilePos = dataFilePos + (fetchDataStreamLen - remainFetchedLen);
				tryParseAndAddCachedRecursiveChildList(res, fetchDataStream, currDataFilePos);
			} catch(Exception ex) {
				log.error("Failed to load more entries from prefetched data.. ignore", ex);
			}
		}
		return res;
	}

	private CachedNodeEntry dataAndChildPosToCachedEntry(NodeName name, long dataFilePos, NodeDataAndChildFilePos dataAndChildPos) {
		CachedNodeEntry res;
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
		return res;
	}

	private void tryParseAndAddCachedRecursiveChildList(CachedNodeEntry node, 
			ByteArrayInputStream remainIn, final long fromDataFilePos) {
		// 'node' data already loaded.. only recurse on child list
		val childNames = node.cachedData.childNames;
		if (childNames != null && !childNames.isEmpty()) {
			val childNameLs = new ArrayList<>(childNames);
			val childNameCount = childNameLs.size();
			
			long currFilePos = fromDataFilePos; // incremented while reading, using remainIn.available()
			
			for(int i = 0; i < childNameCount; i++) {
				val childName = childNameLs.get(i);
				long remainBefore = remainIn.available();
				if (remainBefore < tryParseEntryThresholdSize) {
					break; // do not even try.. will probably fail
				}

				val childEntry = recursiveTryParse(childName, remainIn, currFilePos);

				setLoadedChild(node, i, childName, childEntry);
				
				val remainAfter = remainIn.available();
				currFilePos += remainBefore - remainAfter;
			}
		}
	}

	private void setLoadedChild(CachedNodeEntry node, int i, final fr.an.attrtreestore.api.NodeName childName,
			final fr.an.attrtreestore.storage.impl.Cached_ReadOnlyIndexedBlobStorage_TreeNodeData.CachedNodeEntry childEntry) {
		if (childEntry != null) {
			// ensure update with same name + dataFilePos
			val prevEntry = node.sortedEntries[i];
			if (prevEntry instanceof NodeEntryHandle) {
				val nh = (NodeEntryHandle) prevEntry;
				if (nh.dataFilePos != childEntry.dataFilePos
						|| !nh.name.equals(childName)) {
					throw new IllegalStateException("internal check failed: loaded entry[" + i + "] '" + nh.name + "'" 
							+ " with dataFilePos:" + childEntry.dataFilePos
							+ ", expecting dataFilePos:" + nh.dataFilePos
							+ ((nh.name.equals(childName))? "" : " '" + nh.name + "'")
							);
				}
			} else {
				log.error("should not occur?? loading and entry already loaded (maybe in different threads)");
			}
					
			node.sortedEntries[i] = childEntry;
		}
	}

	private CachedNodeEntry recursiveTryParse(NodeName name, 
			ByteArrayInputStream remainIn, final long fromDataFilePos) {
		long currFilePos = fromDataFilePos; // incremented while reading, using remainIn.available()
		val remainBeforeData = remainIn.available();

		NodeDataAndChildFilePos dataAndChildPos;
		try {
			DataInputStream in = new DataInputStream(remainIn);
			dataAndChildPos = indexedTreeNodeDataEncoder.readNodeDataAndChildIndexes(in, name);
		} catch(EOFException ex) {
			// ok, can occur ..ignore, no rethrow
			return null;
		} catch (IOException ex) {
			log.error("should not occur", ex);
			return null;
		}
		val resEntry = dataAndChildPosToCachedEntry(name, fromDataFilePos, dataAndChildPos);

		val remainAfterData = remainIn.available();
		currFilePos += remainBeforeData - remainAfterData;
		
		// recurse on child list if any
		val childNames = resEntry.cachedData.childNames;
		if (childNames != null && !childNames.isEmpty()) {
			val childNameLs = new ArrayList<>(childNames);
			val childNameCount = childNameLs.size();
						
			for(int i = 0; i < childNameCount; i++) {
				val childName = childNameLs.get(i);
				long remainBeforeChild = remainIn.available();
				if (remainBeforeChild < tryParseEntryThresholdSize) {
					break; // do not even try.. will probably fail
				}
				
				// *** recurse ***
				val childEntry = recursiveTryParse(childName, remainIn, currFilePos);

				setLoadedChild(resEntry, i, childName, childEntry);

				long remainAfterChild = remainIn.available();
				currFilePos += remainAfterChild - remainBeforeChild;
			}
		}

		return resEntry;
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
