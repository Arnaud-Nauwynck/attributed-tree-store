package fr.an.attrtreestore.storage.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import com.google.common.io.CountingInputStream;

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

	private int maxBufferSize = 32 * 1024; // 32ko ... may use 4ko for TCP message: 1 call ~ 4k ??
	private int defaultFetchSize = 128 * 1024; // 128ko ?? ... will force many more calls to storage, to fill cache more aggressively? 
	
	private int tryParseEntryThresholdSize = 100;

	@Getter
	private long cacheMiss = 0;
	@Getter
	private long cacheHit = 0;
	
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
			IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder,
			IndexedBlobStorageInitMode initMode, long initPrefetchSize) {
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

		// caller MUST call init next cf next
		// TOADD preload, maybe few others recursively..
		if (initMode != IndexedBlobStorageInitMode.NOT_INITIALIZED) {
			init(initMode, initPrefetchSize);
		}
	}

	@Override
	public void init(IndexedBlobStorageInitMode initMode, long initPrefetchSizeArgs) {
		switch(initMode) {
		case RELOAD_ROOT_ONLY:
			initReloadRoot(initPrefetchSizeArgs);
			break;

		case RELOAD_FULL:
			// current restriction... only 2g for int=2^32
			initReloadRoot(fileLen);
			break;
			
		case NOT_INITIALIZED:
			// do nothing
			break;
		}
	}

	protected void initReloadRoot(long initFetchSize) {
		// initialize rootNode content!! (data + corresponding sortedEntries)
		val rootHandle = new NodeEntryHandle(this.rootNode.name, this.rootNode.dataFilePos);
		val loadedRootNode = doLoadCachedNodeEntry(rootHandle, initFetchSize);
		synchronized(this.rootNode) {
			this.rootNode.cachedData = loadedRootNode.cachedData;
			this.rootNode.sortedEntries = loadedRootNode.sortedEntries;
		}
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
				cacheHit++;
			} else { // NodeEntryHandle
				val childEntryHandle = (NodeEntryHandle) childObj;
				// cache miss .. need to async reload entry from cache
				cacheMiss++;
				CachedNodeEntry childEntry = doLoadCachedNodeEntry(childEntryHandle, defaultFetchSize);
				currEntry.sortedEntries[childIdx] = childEntry;
				currEntry = childEntry;
			}
		}
		NodeData res = currEntry.cachedData;
		if (res == null) {
			// cache miss on last entry on cachedData
			cacheMiss++;
			val currHandle = new NodeEntryHandle(currEntry.name, currEntry.dataFilePos);
			CachedNodeEntry reloadCurrEntry = doLoadCachedNodeEntry(currHandle, defaultFetchSize);
			res = reloadCurrEntry.cachedData;
		}
		return res;
	} 

	// ------------------------------------------------------------------------
	
	private CachedNodeEntry doLoadCachedNodeEntry(NodeEntryHandle entryHandle, long fetchSizeArgs) {
		CachedNodeEntry res;
		val dataFilePos = entryHandle.dataFilePos;
		long maxReadLen = fileLen - dataFilePos;
		long maxReadCount = Math.min(fetchSizeArgs, maxReadLen);
		
		int bufferSize = (maxReadCount < maxBufferSize)? (int)maxReadCount : maxBufferSize;
		// if bufferSize < 4096? ... not efficient?
		
		try (InputStream storageInputStream = blobStorage.openRead(fileName, dataFilePos)) {
			BufferedInputStream bufferedStorageIn = new BufferedInputStream(storageInputStream, bufferSize);
			
			CountingInputStream counting = new CountingInputStream(bufferedStorageIn);
			DataInputStream in = new DataInputStream(counting);
		
			val name = entryHandle.name;
			NodeDataAndChildFilePos dataAndChildPos = null;
		
			dataAndChildPos = indexedTreeNodeDataEncoder.readNodeDataAndChildIndexes(in, name);
				
			res = dataAndChildPosToCachedEntry(name, dataFilePos, dataAndChildPos);
		
			long currReadCount = counting.getCount();
			if (currReadCount < maxReadCount) {
				// also try parse recursively few other entries from buffer / re-filled buffer
				try {
					tryParseAndAddCachedRecursiveChildList(res, in, dataFilePos, counting, maxReadCount);
				} catch(Exception ex) {
					log.error("Failed to load more entries from prefetched data.. ignore", ex);
				}
			}
		} catch(IOException ex) {
			throw new RuntimeException("Failed to read entry " + entryHandle.name + " at " + dataFilePos, ex);
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
			DataInputStream dataIn,
			long countDataFilePosOffset, CountingInputStream counting, long maxReadCount
			) {
		// 'node' data already loaded.. only recurse on child list
		val childNames = node.cachedData.childNames;
		if (childNames != null && !childNames.isEmpty()) {
			val childNameLs = new ArrayList<>(childNames);
			val childNameCount = childNameLs.size();
						
			for(int i = 0; i < childNameCount; i++) {
				val childName = childNameLs.get(i);
				
				long currCount = counting.getCount();
				if (currCount > maxReadCount) {
					break; // got enough, stop reading
				}

				val childEntry = recursiveTryParse(childName, dataIn, countDataFilePosOffset, counting, maxReadCount);

				setLoadedChild(node, i, childName, childEntry);
			}
		}
	}

	private void setLoadedChild(CachedNodeEntry node, int i, NodeName childName,
			CachedNodeEntry childEntry) {
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
			DataInputStream dataIn,
			long countDataFilePosOffset, CountingInputStream counting, long maxReadCount
			) {
		val dataFilePos = countDataFilePosOffset + counting.getCount();
		NodeDataAndChildFilePos dataAndChildPos;
		try {
			dataAndChildPos = indexedTreeNodeDataEncoder.readNodeDataAndChildIndexes(dataIn, name);
		} catch(EOFException ex) {
			// ok, can occur ..ignore, no rethrow
			return null;
		} catch (IOException ex) {
			log.error("should not occur", ex);
			return null;
		}
		val resEntry = dataAndChildPosToCachedEntry(name, dataFilePos, dataAndChildPos);

		// recurse on child list if any
		val childNames = resEntry.cachedData.childNames;
		if (childNames != null && !childNames.isEmpty()) {
			val childNameLs = new ArrayList<>(childNames);
			val childNameCount = childNameLs.size();
						
			for(int i = 0; i < childNameCount; i++) {
				val childName = childNameLs.get(i);

				long currCount = counting.getCount();
				if (currCount > maxReadCount) {
					break; // got enough, stop reading
				}
				
				// *** recurse ***
				val childEntry = recursiveTryParse(childName, dataIn, countDataFilePosOffset, counting, maxReadCount);

				setLoadedChild(resEntry, i, childName, childEntry);
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
