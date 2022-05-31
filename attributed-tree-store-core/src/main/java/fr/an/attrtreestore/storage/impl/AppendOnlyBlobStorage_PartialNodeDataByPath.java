package fr.an.attrtreestore.storage.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.CountingInputStream;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.api.NodeOverrideData;
import fr.an.attrtreestore.storage.api.NodeOverrideStatus;
import fr.an.attrtreestore.storage.api.PartialNodeDataByPath;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * roughly equivalent to persisted <code> Map<NodeNamesPath, NodeData> </code>
 * 
 * Internally, only appends are performed to underlying BlobStorage,
 * so intermediate updates file should be recompacted at regular intervals.
 * 
 * for Memory footprint: avoid a unique big 'HashMap', containing lots of 'NodeNamesPath' (themselves array of NodeName)
 * all entries are in-memory but 'NodeData' themselves can be evicted/reloaded from file at any time.
 * using internally partial tree node 'PartialNodeEntry', containing 'Map<NodeName, PartialNodeEntry>'
 */
@Slf4j
public class AppendOnlyBlobStorage_PartialNodeDataByPath extends PartialNodeDataByPath {

	private static final String FILE_HEADER = "append-path-data";

	@AllArgsConstructor
	private static class PartialNodeEntry {
		private final NodeName name; // for debug only, else could be implicit..
		private NodeOverrideStatus overrideStatus;
		private Map<NodeName,PartialNodeEntry> child;
		private long dataFilePos;
		private int dataLen;
		private NodeData cachedData;
		
		void setOverrideData(NodeOverrideStatus overrideStatus, long dataFilePos, int dataLen, NodeData cachedData) {
			this.overrideStatus = overrideStatus;
			this.dataFilePos = dataFilePos;
			this.dataLen = dataLen;
			this.cachedData = cachedData;
		}
		
		@Override
		public String toString() {
			return "PartialNodeEntry [" + name + " " 
					+ overrideStatus 
					+ ((child != null && !child.isEmpty())? child.size() + " child" : "")
					+ "]";
		}
		
	}
	
	private final BlobStorage blobStorage;
	@Getter
	private final String baseFileName;

	private final AttrDataEncoderHelper attrDataEncoderHelper; 
	
	private final PartialNodeEntry rootEntry = new PartialNodeEntry(null, NodeOverrideStatus.NOT_OVERRIDEN, new HashMap<>(), 0L, 0, null);

	private String currFileName;

	private final Object writeLock = new Object();
	// @GuardedBy("writeLock")
	private String currPathSlash = "";
	// @GuardedBy("writeLock")
	private long currFilePos = 0;
	
	// ------------------------------------------------------------------------

	public AppendOnlyBlobStorage_PartialNodeDataByPath(BlobStorage blobStorage, String baseFileName,
			AttrInfoIndexes attrIndexes,
			NodeNameEncoder nodeNameEncoder) {
		this.blobStorage = blobStorage;
		this.baseFileName = baseFileName;
		this.attrDataEncoderHelper = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder);
		this.currFileName = baseFileName + "-0.data";
	}
	
	public void initCreateEmptyOrReload() {
		if (! blobStorage.exists(currFileName)) {
			initCreateEmpty();
		} else {
			initReload();
		}
	}

	public void initCreateEmpty() {
		// create empty file
		byte[] header = FILE_HEADER.getBytes(); 
		blobStorage.writeFile(currFileName, header);
		this.currFilePos = header.length;
	}

	public void initReload() {
		// TODO .. should aquire file lock..
		val headerLen = FILE_HEADER.getBytes().length;
		this.currFilePos = headerLen;
		val currFileLen = blobStorage.fileLen(currFileName);
		doReloadFileRange(headerLen, currFileLen);
	}
	
	private void doReloadFileRange(final long fromFilePos, final long toFilePos) {
		synchronized(writeLock) {
			try (val fileIn = blobStorage.openRead(currFileName, fromFilePos)) {
				val inCounter = new CountingInputStream(new BufferedInputStream(fileIn));
				val in = new DataInputStream(inCounter);
				
				NodeNameEncoder nodeNameEncoder = attrDataEncoderHelper.nodeNameEncoder;
				
				val toCount = toFilePos - fromFilePos;
				while(inCounter.getCount() < toCount) {
					
					val pathText = AttrDataEncoderHelper.readIncrString(in, currPathSlash);
					this.currPathSlash = pathText;
					val path = nodeNameEncoder.encodePath(pathText); 
							
					val statusByte = in.read();
					if (statusByte == ' ') { // for NodeOverrideStatus.UPDATED
						// take current filePos
						long dataFilePos = fromFilePos + inCounter.getCount();
								
						val name = path.pathElements[path.pathElements.length-1];
						val data = attrDataEncoderHelper.readNodeData_noName(in, name);

						// current filePos
						long filePosAfterData = fromFilePos + inCounter.getCount();
						int dataLen = (int) (filePosAfterData - dataFilePos);

						// TOADD read (redundant) dataLen for skippable ? 
						
						doReplayPut(path, dataFilePos, dataLen, data);
					} else if (statusByte == '-') { // for NodeOverrideStatus.DELETED
						doReplayRemoved(path);
					} else {
						throw new IllegalStateException("should not occur");
					}
				}

				// TOADD check..
				this.currFilePos = fromFilePos + inCounter.getCount(); 
				if (toFilePos != this.currFilePos) {
					log.error("should not occur? currFilePos " + currFilePos + " != " + toFilePos);
				}

			} catch(Exception ex) {
				throw new RuntimeException("Failed to read", ex);
			}
		}
	}

	// implements api
	// ------------------------------------------------------------------------

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		if (data == null) {
			remove(path);
			return;
		}
		String pathSlash = path.toPathSlash();
		val dataBuffer = new ByteArrayOutputStream(4096);

		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		PartialNodeEntry entry = resolveMkEntry(path, true);

		long dataFilePos;
		int dataLen;
		synchronized(writeLock) {
			// compute byte data payload to be appended
			try(val out = new DataOutputStream(dataBuffer)) {
				// encode: {(incremental)path, overrideStatus, data}
				AttrDataEncoderHelper.writeIncrString(out, pathSlash, currPathSlash);
				this.currPathSlash = pathSlash;

				out.write(' '); // char for NodeOverrideStatus.UPDATED
				
				out.flush();
				val bufferLenBeforeData = dataBuffer.size();
				dataFilePos = this.currFilePos + bufferLenBeforeData;
				// take current filePos
				// dataFilePos = bufferLenBefore;

				// TOADD write (redundant) dataLen for skippable ? 
				// out.writeInt(dataLen);
				
				attrDataEncoderHelper.writeNodeData_noName(out, data);
				
				out.flush();
				val bufferLenAfterData = dataBuffer.size();
				dataLen = bufferLenAfterData - bufferLenBeforeData;
			} catch (IOException ex) {
				throw new RuntimeException("Failed to encode", ex); // should not occur.. encode only!
			}
			
			// do write append
			byte[] filePart = dataBuffer.toByteArray();
			this.blobStorage.writeAppendToFile(currFileName, filePart);
			this.currFilePos += filePart.length;
			
			synchronized(entry) { // useless redundant lock?
				// update in-memory: mark as 'UPDATED'
				entry.setOverrideData(NodeOverrideStatus.UPDATED, dataFilePos, dataLen, data);
			}
		} // synchronized writeLock
	}
	
	private void doReplayPut(NodeNamesPath path, long dataFilePos, int dataLen, NodeData cachedData) {
		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		PartialNodeEntry entry = resolveMkEntry(path, true);
		
		synchronized(entry) { // useless redundant lock?
			// update in-memory: mark as 'UPDATED'
			entry.setOverrideData(NodeOverrideStatus.UPDATED, dataFilePos, dataLen, cachedData);
		}
	}
	
	@Override
	public void remove(NodeNamesPath path) {
		String pathSlash = path.toPathSlash();
		val dataBuffer = new ByteArrayOutputStream(4096);

		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		PartialNodeEntry entry = resolveMkEntry_Deleted(path);
		if (entry == null) {
			return;
		}
		
		Map<NodeName,PartialNodeEntry> gcChildMap = null;
		synchronized(writeLock) {
			// compute byte data payload to be appended
			try(val out = new DataOutputStream(dataBuffer)) {
				// encode: {(incremental)path, overrideStatus, data}
				AttrDataEncoderHelper.writeIncrString(out, pathSlash, currPathSlash);
				this.currPathSlash = pathSlash;

				out.write('-'); // char for NodeOverrideStatus.DELETED
				
			} catch (IOException ex) {
				throw new RuntimeException("Failed to encode", ex); // should not occur.. encode only!
			}
			
			// do write append
			byte[] filePart = dataBuffer.toByteArray();
			this.blobStorage.writeAppendToFile(currFileName, filePart);
			this.currFilePos += filePart.length;
			
			// update in-memory: mark as 'DELETED' + remove all sub-child if any
			synchronized(entry) { // useless redundant lock?
				entry.setOverrideData(NodeOverrideStatus.DELETED, 0, 0, null);
				
				// remove all sub-child overrides if any
				gcChildMap = entry.child;  // => optim, to help GC (?)
				entry.child = null; // => implicit remove all
			}
		} // synchronized writeLock
		
		// help gc: free references
		if (gcChildMap != null) {
			recursiveClearMap(gcChildMap);
		}
	}
	
	private void doReplayRemoved(NodeNamesPath path) {
		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		PartialNodeEntry entry = resolveMkEntry_Deleted(path);
		if (entry == null) {
			return;
		}
		
		Map<NodeName,PartialNodeEntry> gcChildMap = null;
			
		// update in-memory: mark as 'DELETED' + remove all sub-child if any
		synchronized(entry) { // useless redundant lock?
			entry.setOverrideData(NodeOverrideStatus.DELETED, 0, 0, null);
			
			// remove all sub-child overrides if any
			gcChildMap = entry.child;  // => optim, to help GC (?)
			entry.child = null; // => implicit remove all
		}
		
		// help gc: free references
		if (gcChildMap != null) {
			recursiveClearMap(gcChildMap);
		}
	}
	
	private static void recursiveClearMap(Map<NodeName,PartialNodeEntry> map) {
		for (val e: map.values()) {
			if (e.child != null) {
				recursiveClearMap(e.child);
			}
		}
		map.clear();
	}
	
	@Override
	public NodeOverrideData get(NodeNamesPath path) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		PartialNodeEntry currEntry = rootEntry;
		// implicit.. NodeName currName = null;
		for(int i = 0; i < pathEltCount; i++) {
			if (currEntry.child == null) {
				return NodeOverrideData.NOT_OVERRIDEN; 
			}
			val pathElt = pathElts[i];
			val foundChild = currEntry.child.get(pathElt);
			if (foundChild == null) {
				return NodeOverrideData.NOT_OVERRIDEN; 
			} else if (foundChild.overrideStatus == NodeOverrideStatus.DELETED) {
				return NodeOverrideData.DELETED; 
			}
			currEntry = foundChild;
			// implicit.. currName = pathElt;
		}
		val currName = pathElts[pathEltCount-1];
		if (currEntry == null) { // should not occur
			return NodeOverrideData.NOT_OVERRIDEN; 
		}
		if (currEntry.overrideStatus == NodeOverrideStatus.DELETED) {
			return NodeOverrideData.DELETED;
		} else if (currEntry.overrideStatus == NodeOverrideStatus.UPDATED) {
			val cachedData = currEntry.cachedData;
			if (cachedData != null) {
				return new NodeOverrideData(NodeOverrideStatus.UPDATED, cachedData);
			} else {
				// need reload data from cache, using filePos
				// TOCHANGE.. should return a Future<NodeOverrideData> ??
				// current impl: blocking read

				// **** The Biggy: IO Read (maybe remote) ***
				val reloadedData = doReadData(currEntry, currName);

				return new NodeOverrideData(NodeOverrideStatus.UPDATED, reloadedData);
			}
		} else {
			throw new IllegalStateException(); // should not occur
		}
		
	}

	// ------------------------------------------------------------------------
	
	private PartialNodeEntry resolveMkEntry(NodeNamesPath path, boolean setIntermediateEntryUpdated) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		PartialNodeEntry currEntry = rootEntry;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			synchronized(currEntry) {
				if (setIntermediateEntryUpdated && i+1 < pathEltCount) {
					if (currEntry.overrideStatus != NodeOverrideStatus.UPDATED) {
						currEntry.overrideStatus = NodeOverrideStatus.UPDATED;
					}
				}
				
				if (currEntry.child == null) {
					currEntry.child = new HashMap<>(); 
				}
				val foundChild = currEntry.child.get(pathElt);
				if (foundChild == null) {
					val child = new PartialNodeEntry(
							pathElt, // for debug only, else could be implicit..
							NodeOverrideStatus.NOT_OVERRIDEN,
							new HashMap<NodeName,PartialNodeEntry>(1),
							0, 0, null // dataFilePos, dataLen, cachedData
							);
					currEntry.child.put(pathElt, child);
					currEntry = child; 
				} else {
					currEntry = foundChild; 
				}
			}
		}
		return currEntry;
	}
	
	private PartialNodeEntry resolveMkEntry_Deleted(NodeNamesPath path) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		PartialNodeEntry currEntry = rootEntry;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			synchronized(currEntry) {
				if (i+1 < pathEltCount) {
					if (currEntry.overrideStatus == NodeOverrideStatus.DELETED) {
						// parent already deleted => deleting sub-child should have no effect!!
						return null;
					}
					if (currEntry.overrideStatus != NodeOverrideStatus.UPDATED) {
						currEntry.overrideStatus = NodeOverrideStatus.UPDATED;
					}
				}
				
				if (currEntry.child == null) {
					currEntry.child = new HashMap<>(); 
				}
				val foundChild = currEntry.child.get(pathElt);
				if (foundChild == null) {
					val child = new PartialNodeEntry(
							pathElt, // for debug only, else could be implicit..
							NodeOverrideStatus.NOT_OVERRIDEN,
							new HashMap<NodeName,PartialNodeEntry>(1),
							0, 0, null // dataFilePos, dataLen, cachedData
							);
					currEntry.child.put(pathElt, child);
					currEntry = child; 
				} else {
					currEntry = foundChild; 
				}
			}
		}
		return currEntry;
	}
	
	
	private NodeData doReadData(PartialNodeEntry entry, NodeName name) {
		val dataFilePos = entry.dataFilePos;
		val dataLen = entry.dataLen;
		
		// *** The Biggy: IO read (maybe remote) ***
		byte[] dataBytes = blobStorage.readAt(currFileName, dataFilePos, dataLen);

		// decode byte[] -> NodeData
		NodeData res;
		try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(dataBytes))) {
			res = this.attrDataEncoderHelper.readNodeData_noName(in, name);
		} catch(IOException ex) {
			throw new RuntimeException("Failed to decode", ex); // should not occur.. decode only!
		}

		entry.cachedData = res;
		return res;
	}

}
