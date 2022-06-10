package fr.an.attrtreestore.storage.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.CountingInputStream;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeData.NodeDataInternalFields;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.api.override.OverrideNodeData;
import fr.an.attrtreestore.api.override.OverrideNodeStatus;
import fr.an.attrtreestore.api.override.OverrideTreeData;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoderOptions;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
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
public class WALBlobStorage_OverrideTreeData extends OverrideTreeData {

	private static final String FILE_HEADER = "wal-override-tree-data";

	private static final byte ENTRY_NODE_UPDATE = ' '; 
	private static final byte ENTRY_NODE_REMOVED = '-';
	private static final byte ENTRY_NODE_ATTR_CHANGED = 'a';
	private static final byte ENTRY_NODE_INTERNAL_FIELDS_CHANGED = 'i';

	@AllArgsConstructor
	private static class OverrideNodeEntry {
		private final NodeName name; // for debug only, else could be implicit..
		private OverrideNodeStatus overrideStatus;
		private Map<NodeName,OverrideNodeEntry> child;
		private long dataFilePos;
		private int dataLen;
		private NodeData cachedData;
		private NodeDataInternalFields internalFields;
		
		void setMarkDisposed() {
			this.overrideStatus = null; // NodeOverrideStatus.INTERNAL_ENTRY_DISPOSED; // ??
			this.child = null;
			this.cachedData = null;
		}
		
		void setOverrideData(OverrideNodeStatus overrideStatus, long dataFilePos, int dataLen, NodeData cachedData) {
			this.overrideStatus = overrideStatus;
			this.dataFilePos = dataFilePos;
			this.dataLen = dataLen;
			this.cachedData = cachedData;
		}
		
		void setOverrideInternalFields(NodeDataInternalFields internalFields) {
			if (cachedData != null) {
				cachedData.setInternalFields(internalFields);
			} else {
				this.internalFields = internalFields;
			}
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
	private final String fileName;

	private final AttrDataEncoderHelper attrDataEncoderHelper; 
	
	private final OverrideNodeEntry rootEntry = new OverrideNodeEntry(null, OverrideNodeStatus.NOT_OVERRIDEN, new HashMap<>(), 0L, 0, null, null);

	private final Object writeLock = new Object();

	// @GuardedBy("writeLock")
	private String currPathSlash = "";
	// @GuardedBy("writeLock")
	private long currFilePos = 0;
	
	private OutputStream currWriteStream;
	
	// ------------------------------------------------------------------------

	public WALBlobStorage_OverrideTreeData(BlobStorage blobStorage, String fileName,
			AttrDataEncoderHelper attrDataEncoderHelper) {
		this.blobStorage = blobStorage;
		this.fileName = fileName;
		this.attrDataEncoderHelper = attrDataEncoderHelper;
	}

	public void initCreateEmptyOrReload() {
		if (! blobStorage.exists(fileName)) {
			initCreateEmpty();
		} else {
			initReload();
		}
	}

	public void initCreateEmpty() {
		// create empty file
		byte[] header = FILE_HEADER.getBytes(); 
		blobStorage.writeFile(fileName, header);
		this.currFilePos = header.length;
	}

	public void initReload() {
		// TODO .. should aquire file lock..
		val headerLen = FILE_HEADER.getBytes().length;
		this.currFilePos = headerLen;
		val currFileLen = blobStorage.fileLen(fileName);
		doReloadFileRange(headerLen, currFileLen);
	}
	
	public void startWrite() {
		// stream will be opened on first use
	}

	public void flushStopWrite() {
		synchronized(writeLock) {
			if (currWriteStream != null) {
				try {
					currWriteStream.flush();
				} catch (IOException ex) {
					log.error("Failed flush WAL file (last entry might be corrupted?) !!", ex);
				}
				// TOADD wait writer thread if any
				
				try {
					currWriteStream.close();
				} catch (IOException ex) {
					log.error("Failed close WAL file (last entry might be corrupted?) !!", ex);
				}
				currWriteStream = null;
			}
		}
	}



	// implements PartialOverrideTreeData (read part)
	// ------------------------------------------------------------------------
	
	// TODO may use async
	// public CompletableFuture<NodeOverrideData> asyncGetOverride(NodeNamesPath path) {
	
	@Override
	public OverrideNodeData getOverride(NodeNamesPath path) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		OverrideNodeEntry currEntry = rootEntry;
		// implicit.. NodeName currName = null;
		for(int i = 0; i < pathEltCount; i++) {
			if (currEntry.child == null) {
				return OverrideNodeData.NOT_OVERRIDEN; 
			}
			val pathElt = pathElts[i];
			val foundChild = currEntry.child.get(pathElt);
			if (foundChild == null) {
				return OverrideNodeData.NOT_OVERRIDEN; 
			} else if (foundChild.overrideStatus == OverrideNodeStatus.DELETED) {
				return OverrideNodeData.DELETED; 
			}
			currEntry = foundChild;
			// implicit.. currName = pathElt;
		}
		if (currEntry == null) { // should not occur
			return OverrideNodeData.NOT_OVERRIDEN; 
		}
		if (currEntry.overrideStatus == OverrideNodeStatus.UPDATED) {
			val cachedData = currEntry.cachedData;
			if (cachedData != null) {
				return new OverrideNodeData(OverrideNodeStatus.UPDATED, cachedData);
			} else {
				// need reload data from cache, using filePos
				// TOCHANGE.. should return a Future<NodeOverrideData> ??
				// current impl: blocking read
			    val currName = path.lastNameOrEmpty();

				// **** The Biggy: IO Read (maybe remote) ***
				val reloadedData = doReadData(currEntry, currName);

				return new OverrideNodeData(OverrideNodeStatus.UPDATED, reloadedData);
			}
		} else if (currEntry.overrideStatus == OverrideNodeStatus.DELETED) {
		    return OverrideNodeData.DELETED;
		} else { // if (OverrideNodeData.NOT_OVERRIDEN)
		    if (currEntry == rootEntry) {     
		        return null; // ?
    		} else {
    			throw new IllegalStateException(); // should not occur
    		}
		}
	}
	
	// implements api IWriteTreeData
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
		OverrideNodeEntry entry = resolveMkEntry(path, true);

		long dataFilePos;
		int dataLen;
		synchronized(writeLock) {
			// compute byte data payload to be appended
			try(val out = new DataOutputStream(dataBuffer)) {
				// encode: {(incremental)path, overrideStatus, data}
				AttrDataEncoderHelper.writeIncrString(out, pathSlash, currPathSlash);
				this.currPathSlash = pathSlash;

				out.write(ENTRY_NODE_UPDATE);
				
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
			writeAppendToWal(filePart, 0, filePart.length);
			
			synchronized(entry) { // useless redundant lock?
				// update in-memory: mark as 'UPDATED'
				entry.setOverrideData(OverrideNodeStatus.UPDATED, dataFilePos, dataLen, data);
			}
		} // synchronized writeLock
	}
	
	@Override
	public void put_transientFieldsChanged(NodeNamesPath path, NodeData data) {
		String pathSlash = path.toPathSlash();
		val dataBuffer = new ByteArrayOutputStream(4096);

		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		OverrideNodeEntry entry = resolveMkEntry(path, true);

		long dataFilePos;
		int dataLen;
		synchronized(writeLock) {
			// compute byte data payload to be appended
			try(val out = new DataOutputStream(dataBuffer)) {
				// encode: {(incremental)path, overrideStatus, data}
				AttrDataEncoderHelper.writeIncrString(out, pathSlash, currPathSlash);
				this.currPathSlash = pathSlash;

				out.write(ENTRY_NODE_INTERNAL_FIELDS_CHANGED);
				
				out.flush();
				val bufferLenBeforeData = dataBuffer.size();
				dataFilePos = this.currFilePos + bufferLenBeforeData;
				// take current filePos
				// dataFilePos = bufferLenBefore;

				// TOADD write previous filePos
				
				attrDataEncoderHelper.writeNodeData_internalFields(out, data.toInternalFields());
				
				out.flush();
				val bufferLenAfterData = dataBuffer.size();
				dataLen = bufferLenAfterData - bufferLenBeforeData;
			} catch (IOException ex) {
				throw new RuntimeException("Failed to encode", ex); // should not occur.. encode only!
			}
			
			// do write append
			byte[] filePart = dataBuffer.toByteArray();
			writeAppendToWal(filePart, 0, filePart.length);
			
			synchronized(entry) { // useless redundant lock?
				// update in-memory: mark as 'UPDATED'
				entry.setOverrideData(OverrideNodeStatus.UPDATED, dataFilePos, dataLen, data);
			}
		} // synchronized writeLock
	}


	
	@Override
	public void remove(NodeNamesPath path) {
		String pathSlash = path.toPathSlash();
		val dataBuffer = new ByteArrayOutputStream(4096);

		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		OverrideNodeEntry entry = resolveMkEntry_Deleted(path);
		if (entry == null) {
			return;
		}
		
		Map<NodeName,OverrideNodeEntry> recursiveDisposeChildMap = null;
		synchronized(writeLock) {
			// compute byte data payload to be appended
			try(val out = new DataOutputStream(dataBuffer)) {
				// encode: {(incremental)path, overrideStatus, data}
				AttrDataEncoderHelper.writeIncrString(out, pathSlash, currPathSlash);
				this.currPathSlash = pathSlash;

				out.write(ENTRY_NODE_REMOVED);
				
			} catch (IOException ex) {
				throw new RuntimeException("Failed to encode", ex); // should not occur.. encode only!
			}
			
			// do write append
			byte[] filePart = dataBuffer.toByteArray();
			writeAppendToWal(filePart, 0, filePart.length);
			
			// update in-memory: mark as 'DELETED' + remove all sub-child if any
			synchronized(entry) { // useless redundant lock?
				entry.setOverrideData(OverrideNodeStatus.DELETED, 0, 0, null);
				
				// remove all sub-child overrides if any
				recursiveDisposeChildMap = entry.child;
				entry.setMarkDisposed();
			}
		} // synchronized writeLock
		
		// change status to null (INTERNAL_ENTRY_DISPOSED) + help gc by clearing references
		if (recursiveDisposeChildMap != null) {
			recursiveMarkDisposed(recursiveDisposeChildMap);
		}
	}


	// internal
	// ------------------------------------------------------------------------
	
	protected OverrideNodeEntry resolveMkEntry(NodeNamesPath path, boolean setIntermediateEntryUpdated) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		OverrideNodeEntry currEntry = rootEntry;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			synchronized(currEntry) {
				if (setIntermediateEntryUpdated && i+1 < pathEltCount) {
					if (currEntry.overrideStatus != OverrideNodeStatus.UPDATED) {
						currEntry.overrideStatus = OverrideNodeStatus.UPDATED;
					}
				}
				
				if (currEntry.child == null) {
					currEntry.child = new HashMap<>(); 
				}
				val foundChild = currEntry.child.get(pathElt);
				if (foundChild == null) {
					val child = new OverrideNodeEntry(
							pathElt, // for debug only, else could be implicit..
							OverrideNodeStatus.NOT_OVERRIDEN,
							new HashMap<NodeName,OverrideNodeEntry>(1),
							0, 0, // dataFilePos, dataLen
							null, null 
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
	
	protected OverrideNodeEntry resolveMkEntry_Deleted(NodeNamesPath path) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		OverrideNodeEntry currEntry = rootEntry;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			synchronized(currEntry) {
				if (i+1 < pathEltCount) {
					if (currEntry.overrideStatus == OverrideNodeStatus.DELETED) {
						// parent already deleted => deleting sub-child should have no effect!!
						return null;
					}
					if (currEntry.overrideStatus != OverrideNodeStatus.UPDATED) {
						currEntry.overrideStatus = OverrideNodeStatus.UPDATED;
					}
				}
				
				if (currEntry.child == null) {
					currEntry.child = new HashMap<>(); 
				}
				val foundChild = currEntry.child.get(pathElt);
				if (foundChild == null) {
					val child = new OverrideNodeEntry(
							pathElt, // for debug only, else could be implicit..
							OverrideNodeStatus.NOT_OVERRIDEN,
							new HashMap<NodeName,OverrideNodeEntry>(1),
							0, 0,  // dataFilePos, dataLen
							null, null
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
	
	/** read and replay events (in same order) from appended-only Log file from BlobStorage */
	protected void doReloadFileRange(final long fromFilePos, final long toFilePos) {
		synchronized(writeLock) {
			try (val fileIn = blobStorage.openRead(fileName, fromFilePos)) {
				val inCounter = new CountingInputStream(new BufferedInputStream(fileIn));
				val in = new DataInputStream(inCounter);
				
				NodeNameEncoder nodeNameEncoder = attrDataEncoderHelper.nodeNameEncoder;
				
				val toCount = toFilePos - fromFilePos;
				while(inCounter.getCount() < toCount) {
					
				    val pathText = AttrDataEncoderHelper.readIncrString(in, currPathSlash);
					this.currPathSlash = pathText;
					val path = (currPathSlash.isEmpty())? NodeNamesPath.ROOT  
					        : nodeNameEncoder.encodePath(pathText);
							 												
					val chgByte = in.read();
					if (chgByte == ENTRY_NODE_UPDATE) {
						// take current filePos
						long dataFilePos = fromFilePos + inCounter.getCount();
								
						val name = path.lastNameOrEmpty();
						val data = attrDataEncoderHelper.readNodeData_noName(in, name);

						// current filePos
						long filePosAfterData = fromFilePos + inCounter.getCount();
						int dataLen = (int) (filePosAfterData - dataFilePos);

						// TOADD read (redundant) dataLen for skippable ? 
						
						doReplayPut(path, dataFilePos, dataLen, data);

					} else if (chgByte == ENTRY_NODE_REMOVED) {
						doReplayRemoved(path);

					} else if (chgByte == ENTRY_NODE_INTERNAL_FIELDS_CHANGED) {
						val internalFields = attrDataEncoderHelper.readNodeData_internalFields(in);
						doReplayInternalFieldsChanged(path, internalFields);

					} else if (chgByte == ENTRY_NODE_ATTR_CHANGED) {
						// NOT IMPL YET..

					} else {
						throw new IllegalStateException("should not occur");
					}
					
					// last successfully read
					// if failing to read an event (may throw EOF) => currFilePos not updated with partial.. shoud re-seek to currFilePos, then retry read?  
					this.currFilePos = fromFilePos + inCounter.getCount(); 
				}

				// TOADD check..
				if (toFilePos != this.currFilePos) {
					log.error("should not occur? currFilePos " + currFilePos + " != " + toFilePos);
				}

			} catch(Exception ex) {
				throw new RuntimeException("Failed to read", ex);
			}
		}
	}
	
	protected void doReplayPut(NodeNamesPath path, long dataFilePos, int dataLen, NodeData cachedData) {
		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		OverrideNodeEntry entry = resolveMkEntry(path, true);
		
		synchronized(entry) { // useless redundant lock?
			// update in-memory: mark as 'UPDATED'
			entry.setOverrideData(OverrideNodeStatus.UPDATED, dataFilePos, dataLen, cachedData);
		}
	}
	
	protected void doReplayRemoved(NodeNamesPath path) {
		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		OverrideNodeEntry entry = resolveMkEntry_Deleted(path);
		if (entry == null) {
			return;
		}
		
		Map<NodeName,OverrideNodeEntry> recursiveDisposeChildMap = null;
			
		// update in-memory: mark as 'DELETED' + remove all sub-child if any
		synchronized(entry) { // useless redundant lock?
			entry.setOverrideData(OverrideNodeStatus.DELETED, 0, 0, null);
			
			// remove all sub-child overrides if any
			recursiveDisposeChildMap = entry.child;
			entry.setMarkDisposed();
		}
		
		// change status to null (INTERNAL_ENTRY_DISPOSED) + help gc by clearing references
		if (recursiveDisposeChildMap != null) {
			recursiveMarkDisposed(recursiveDisposeChildMap);
		}
	}
	
	protected void doReplayInternalFieldsChanged(NodeNamesPath path, NodeDataInternalFields internalFields) {
		// update in-memory: resolve (mkdirs) parent node + update/add PartialNodeEntry
		OverrideNodeEntry entry = resolveMkEntry(path, true);
		
		synchronized(entry) { // useless redundant lock?
			entry.setOverrideInternalFields(internalFields);
		}
	}
	
	protected static void recursiveMarkDisposed(Map<NodeName,OverrideNodeEntry> map) {
		for (val e: map.values()) {
			val childMap = e.child;
			e.setMarkDisposed();
			if (childMap != null) {
				recursiveMarkDisposed(childMap);
			}
		}
		map.clear();
	}

	// TODO may use async
	// public CompletableFuture<NodeOverrideData> asyncGetOverride(NodeNamesPath path) {

	protected NodeData doReadData(OverrideNodeEntry entry, NodeName name) {
		val dataFilePos = entry.dataFilePos;
		val dataLen = entry.dataLen;
		
		// *** The Biggy: IO read (maybe remote) ***
		byte[] dataBytes = blobStorage.readAt(fileName, dataFilePos, dataLen);

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

	
	protected void writeAppendToWal(byte[] data, int from, int len) {
		// inneficient for local file: open+write+flush+close... 
		// (but maybe equivalent for distributed blobStorage, like Azure Storage)
		// ... this.blobStorage.writeAppendToFile(fileName, filePart);
		if (this.currWriteStream == null) {
			// open on demand
			this.currWriteStream = blobStorage.openWrite(fileName, true);
			// TOCHECK seek to last checkpoint flushed pos... 
		}
		val posBefore = this.currFilePos;
		// notice .. writing may be done asynchronously, by adding message to a queue, and polling to write in a separate thread
		try {
			this.currWriteStream.write(data, from, len);
		} catch (IOException ex) {
			throw new RuntimeException("Failed to write wal " + fileName + " (prevPos:" + posBefore + ")?? ", ex);
		}
		try {
			this.currWriteStream.flush();
		} catch (IOException ex) {
			throw new RuntimeException("Failed to flush wal " + fileName + " (prevPos:" + posBefore + ")?? ", ex);
		}
		this.currFilePos += len;
	}

}
