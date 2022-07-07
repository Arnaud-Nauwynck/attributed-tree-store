package fr.an.attrtreestore.storage.impl;

import com.google.common.io.CountingInputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Random;

import org.simplestorage4j.api.BlobStorage;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder.NodeDataAndChildFilePos;
import fr.an.attrtreestore.util.MemoryWarningSystem;
import fr.an.attrtreestore.util.MemoryWarningSystem.Listener;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class CachedROIndexedBlobStorage_TreeNodeData extends ROCached_TreeData 
	implements IReadTreeData { // Disposable // ??
	
	protected final BlobStorage blobStorage;

	@Getter
	protected final String fileName;

	protected final IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder;

	protected final CachedNodeEntry rootNode;
	
	protected final long fileLen; // computed from blobStorage + fileName at init

	protected int maxBufferSize = 32 * 1024; // 32ko ... may use 4ko for TCP message: 1 call ~ 4k ??
	protected int defaultFetchSize = 128 * 1024; // 128ko ?? ... will force many more calls to storage, to fill cache more aggressively? 

	@Getter
	protected long cacheMiss = 0;
	@Getter
	protected long cacheHit = 0;
	
	/**
	 * partially loaded Node... 
	 * immutable except for loading children from cache /evicting children out of memory 
	 * 
	 * ... trying to be memory efficient, using (sorted) Arrays instead of Map<> and sub objects..
	 * 
	 */
	@RequiredArgsConstructor //?
	@AllArgsConstructor
	protected static class CachedNodeEntry {
		final NodeName name;
		
		long dataFilePos;
		// int dataLen; // .. redundant with filePos 
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
	protected static class NodeEntryHandle {
		final NodeName name;
		long dataFilePos;
		// private int dataLen; // redundant with filePos..
	}
	
	// ------------------------------------------------------------------------
	
	public CachedROIndexedBlobStorage_TreeNodeData(BlobStorage blobStorage, String fileName,
			IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder,
			IndexedBlobStorageInitMode initMode, long initPrefetchSize) {
		this.blobStorage = blobStorage;
		this.fileName = fileName;
		this.indexedTreeNodeDataEncoder = indexedTreeNodeDataEncoder;

        if (! blobStorage.exists(fileName)) {
            // TODO throw
            log.warn("read-only file not found '" + fileName + "' .. will not load data!");
        }
		this.fileLen = blobStorage.fileLen(fileName);
		
		// init the root node, dataFilePos fixed known in file 
		// field 'rootNode' is final, so must be set in ctor... 
		val rootName = NodeName.EMPTY;
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

		case INIT_EMPTY:
			this.rootNode.sortedEntries = new Object[0];
			break;

		case NOT_INITIALIZED:
			// do nothing
			break;
		}
	}

	protected void initReloadRoot(long initFetchSize) {
		// initialize rootNode content!! (data + corresponding sortedEntries)
		val rootHandle = new NodeEntryHandle(this.rootNode.name, this.rootNode.dataFilePos);
		
		NodeData rootData;
		Object[] rootEntries;
		if (blobStorage.exists(fileName)) {
		    val loadedRootNode = doLoadCachedNodeEntry(rootHandle, initFetchSize);
		    rootData = loadedRootNode.cachedData;
		    rootEntries = loadedRootNode.sortedEntries;
		} else {
		    rootData = null; // ??
		    rootEntries = new Object[0];
		}
		synchronized(this.rootNode) {
			this.rootNode.cachedData = rootData;
			this.rootNode.sortedEntries = rootEntries;
		}
	}
	
	// implements api IReadTreeData
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

	// public api for freeing memory by evicting some internal nodes, reloadable later from cache
	// ------------------------------------------------------------------------
	
	private Listener innerLowMemListener = null;
	
	private static final long MEGA = 1024*1024;
	private FreeMemoryByRandomEvictingSubTreeParams randomEvictingSubTreeParams =
			FreeMemoryByRandomEvictingSubTreeParams.builder()
				.untilFreedMemSize(50 * MEGA)
				.minLevel(3)
				.minSubTreeSizeOnDisk(20 * MEGA)		
				.maxSubTreeSizeOnDisk(50 * MEGA)		
				.build();
	
	public void registerLowMemoryListenerOffloader() {
		if (innerLowMemListener == null) {
			innerLowMemListener = (usedMemory, maxMemory) -> {
				if (randomEvictingSubTreeParams != null) {
					log.info("detected low memory, randomEvictingSubTree ..");
					long estimatedFreed = freeMemoryByRandomEvictingSubTree(randomEvictingSubTreeParams);
					log.info("detected low memory.. randomEvictingSubTree => " + estimatedFreed / MEGA + " Mb");
				}
			};
		}
		MemoryWarningSystem.instance.addListener(innerLowMemListener);
	}

	public void unregisterLowMemoryListenerOffloader() {
		if (innerLowMemListener != null) {
			MemoryWarningSystem.instance.removeListener(innerLowMemListener);
		}
	}

	
	@Builder
	public static class FreeMemoryByRecursiveEvictingEntryParams {
		public long untilFreedMemSize;		
		public int minLevelEvictType1; // for Dir		
		public int minLevelEvictType2; // for File
	}

	@RequiredArgsConstructor
	private static class FreeMemoryByRecursiveEvictingEntryContext {
		private final long untilFreedMemSize;
		private final int minLevelEvictType1;
		private final int minLevelEvictType2;
		
		private int currLevel;
		private long currFreedMemSize;
	}
	
	public long freeMemoryByRecursiveEvictingEntries(
			FreeMemoryByRecursiveEvictingEntryParams params) {
		if (params.untilFreedMemSize == 0) {
			params.untilFreedMemSize = 50 * 1024* 1024;
		}
		val ctx = new FreeMemoryByRecursiveEvictingEntryContext(params.untilFreedMemSize, params.minLevelEvictType1, params.minLevelEvictType2);
		doRecursiveFreeMemory(rootNode, ctx);
		return ctx.currFreedMemSize;
	}
	
	private void doRecursiveFreeMemory(CachedNodeEntry node, FreeMemoryByRecursiveEvictingEntryContext ctx) {
		Object[] childEntries = node.sortedEntries;
		if (childEntries == null || childEntries.length == 0) {
			return;
		}
		ctx.currLevel++;
		try {
			val childCount = childEntries.length;
			for(int i = 0; i < childCount; i++) {
				val e = childEntries[i];
				if (e instanceof CachedNodeEntry) {
					val childNode = (CachedNodeEntry) e;
	
					// *** recurse first (free sub-child, before freeing all tree) ***
					doRecursiveFreeMemory(childNode, ctx);
					
					if (ctx.currFreedMemSize > ctx.untilFreedMemSize) {
						break; // return;
					}
					
					val type = (childNode.cachedData != null)? childNode.cachedData.type : 0; 
					boolean decideEvictEntry = false;
					if (type == 1) {
						if (ctx.currLevel > ctx.minLevelEvictType1) {
							decideEvictEntry = true;
						}
					} else if (type == 2) {
						if (ctx.currLevel > ctx.minLevelEvictType2) {
							decideEvictEntry = true;
						}
					}
					
					if (decideEvictEntry) {
						// cache evict child entry.. replaced by NodeHandle
						val childEntryHandle = new NodeEntryHandle(childNode.name, childNode.dataFilePos);
						childEntries[i] = childEntryHandle;
						int estimateNodeMem = estimateEntryInMemSize(childNode);
						// may help GC by recursively clearing all refs, but need replacing by Handle..
						ctx.currFreedMemSize += estimateNodeMem;
						if (ctx.currFreedMemSize > ctx.untilFreedMemSize) {
							return;
						}
					}

				}
			}
		} finally {
			ctx.currLevel--;
		}
	}

	private int estimateEntryInMemSize(
			final fr.an.attrtreestore.storage.impl.CachedROIndexedBlobStorage_TreeNodeData.CachedNodeEntry childNode) {
		int estimateNodeMem = 120;
		if (childNode.cachedData != null) {
			val data = childNode.cachedData; 
			estimateNodeMem += 50 * data.attrCount() + 16 * data.childCount();
		}
		return estimateNodeMem;
	}
	
	// ------------------------------------------------------------------------
	
	@Builder
	public static class FreeMemoryByRandomEvictingSubTreeParams {
		public long untilFreedMemSize;
		public int minLevel;		
		public long minSubTreeSizeOnDisk;		
		public long maxSubTreeSizeOnDisk;		
	}

	@RequiredArgsConstructor
	private static class FreeMemoryByRandomEvictingSubTreeContext {
		private final long untilFreedMemSize;
		private final int minLevel;		
		private final long minSubTreeSizeOnDisk;		
		private final long maxSubTreeSizeOnDisk;		
		private final Random rand;
		
		private int currLevel;
		private long currFreedMemSize;
	}

	public long freeMemoryByRandomEvictingSubTree(
			FreeMemoryByRandomEvictingSubTreeParams params) {
		if (params.untilFreedMemSize == 0) {
			params.untilFreedMemSize = 50 * 1024* 1024;
		}
		val ctx = new FreeMemoryByRandomEvictingSubTreeContext(
				params.untilFreedMemSize, 
				params.minLevel, 
				params.minSubTreeSizeOnDisk,
				params.maxSubTreeSizeOnDisk,
				new Random()
				);
		for(;;) {
			val prevFreedMemSize = ctx.currFreedMemSize;
			doRandomTraverseUntilFreeSubTree(rootNode, ctx, fileLen);
			val iterationFreedMemSize = ctx.currFreedMemSize - prevFreedMemSize;
			if (iterationFreedMemSize == 0) {
				break;
			}
			if (ctx.currFreedMemSize >= ctx.untilFreedMemSize) {
				break;
			}
		}
		return ctx.currFreedMemSize;
	}
	
	private void doRandomTraverseUntilFreeSubTree(CachedNodeEntry node, 
			FreeMemoryByRandomEvictingSubTreeContext ctx,
			long currLastChildEndFilePos) {
		Object[] childEntries = node.sortedEntries;
		if (childEntries == null || childEntries.length == 0) {
			return;
		}
		ctx.currLevel++;
		try {
			val childCount = childEntries.length;
			
			int maxRetryRand = Math.min(4, childCount);
			for(int retryRandCount = 0; retryRandCount < maxRetryRand; retryRandCount++) {
				int i = ctx.rand.nextInt(childCount);
				Object child = childEntries[i];
				
				long childEndFilePos;
				if (i + 1 < childCount) {
					val nextSibling = childEntries[i+1];
					childEndFilePos = ((nextSibling instanceof CachedNodeEntry))? 
							((CachedNodeEntry) nextSibling).dataFilePos
							: ((NodeEntryHandle) nextSibling).dataFilePos; 
				} else {
					childEndFilePos = currLastChildEndFilePos;
				}
	
				if (!(child instanceof CachedNodeEntry)) {
					// child already freed? try choose another child? else return..
					continue;
				}
			
				val childEntry = (CachedNodeEntry) child;
				
				long childDataFilePos = childEntry.dataFilePos;
				long subTreeSize = childEndFilePos - childDataFilePos;

				boolean freeChild = false;
				if (ctx.currLevel >= ctx.minLevel
						&& subTreeSize >= ctx.minSubTreeSizeOnDisk 
						&& subTreeSize <= ctx.maxSubTreeSizeOnDisk) {
					// ok, free
					freeChild = true;
				} else {
					// recursive dive into, try to free a (random) sub-child..
					val prev = ctx.currFreedMemSize;
					
					// *** recurse ***
					doRandomTraverseUntilFreeSubTree(childEntry, ctx, childEndFilePos);
					
					val freedSubChild = ctx.currFreedMemSize != prev;
					if (!freedSubChild) {
						freeChild = true; 
					}
				}

				if (freeChild) {
					val childEntryHandle = new NodeEntryHandle(childEntry.name, childDataFilePos);
					childEntries[i] = childEntryHandle;
					int estimateNodeMem = estimateEntryInMemSize(childEntry);
					// may help GC by recursively clearing all refs, but need replacing by Handle..
					ctx.currFreedMemSize += estimateNodeMem;
					return;
				}
			}

		} finally {
			ctx.currLevel--;
		}
	}

	
	
	// ------------------------------------------------------------------------
	
	protected CachedNodeEntry doLoadCachedNodeEntry(NodeEntryHandle entryHandle, long fetchSizeArgs) {
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

	
	protected CachedNodeEntry dataAndChildPosToCachedEntry(NodeName name, long dataFilePos, NodeDataAndChildFilePos dataAndChildPos) {
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

	protected void tryParseAndAddCachedRecursiveChildList(CachedNodeEntry node, 
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

	protected void setLoadedChild(CachedNodeEntry node, int i, NodeName childName,
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

	protected CachedNodeEntry recursiveTryParse(NodeName name, 
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
