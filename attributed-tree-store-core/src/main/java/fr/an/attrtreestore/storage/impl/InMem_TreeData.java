package fr.an.attrtreestore.storage.impl;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.path4j.NodeName;
import org.path4j.NodeNamesPath;
import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.util.NoFlushCountingOutputStream;
import org.simplestorage4j.api.util.NullCountingOutputStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory implementation of TreeData, with Read + Write operations : get(path) / put(path,data) / remove(path)
 * 
 * TODO TOCHANGE
 * ... currently also contains fields for computing file position of entries, for writing to indexed file
 * may wrap copy in temporary entries, for these extra fields
 */
@Slf4j
public class InMem_TreeData extends TreeData implements IReadTreeData, IWriteTreeData {

	@Getter
	private final InMemNodeEntry rootNode = new InMemNodeEntry(null, createEmptyRootData(), 0, 0L, 0L, new TreeMap<>());

	// @AllArgsConstructor
	public static class InMemNodeEntry {
		@Getter
		final NodeName name;
		
		NodeData data;
		
		// for saving to indexed file
		// computed from recursive data encoding
		int dataAndChildFilePosLen; // computed from data
		long recursiveDataLenSum; // computed from dataAndChildFilePosLen + recursively scan on sortedEntries 
		long synthetisedDataFilePos; 

		TreeMap<NodeName,InMemNodeEntry> sortedEntries;
		

		public InMemNodeEntry(NodeName name, NodeData data, int dataAndChildFilePosLen, long recursiveDataLenSum,
				long synthetisedDataFilePos, TreeMap<NodeName, InMemNodeEntry> sortedEntries) {
			this.name = name;
			this.data = data;
			this.dataAndChildFilePosLen = dataAndChildFilePosLen;
			this.recursiveDataLenSum = recursiveDataLenSum;
			this.synthetisedDataFilePos = synthetisedDataFilePos;
			this.sortedEntries = sortedEntries;
		}

		public InMemNodeEntry getEntry(NodeName chlidName) {
			if (sortedEntries == null) {
				return null;
			}
			return sortedEntries.get(chlidName);
		}

		public InMemNodeEntry addEntry(NodeData data) {
			InMemNodeEntry res = getOrCreateEntry(data.name);
			res.data = data;
			return res;
		}

		public InMemNodeEntry getOrCreateEntry(NodeName pathElt) {
			InMemNodeEntry res;
			if (sortedEntries == null) {
				sortedEntries = new TreeMap<>(); 
			}
			val found = sortedEntries.get(pathElt);
			if (found != null) {
				res = found;
			} else {
				// if allowAutoCreateParent... 
				res = new InMemNodeEntry(pathElt, null, 0, 0L, 0L, null);
				sortedEntries.put(pathElt, res);
			}
			return res;
		}

		public void removeEntry(NodeName childName) {
			val removed = sortedEntries.remove(childName);
			if (removed == null) {
				// no child removed
			} else {
				// TOADD recursive mark child as deleted.. maybe help GC
			}
		}
		
		@Override
		public String toString() {
			return "FullInMemNodeEntry [" + name 
					+ ((sortedEntries != null && sortedEntries.size() != 0)? ", " + sortedEntries.size() + " child" : "")
					+ "]";
		}
		
	}
	
	// ------------------------------------------------------------------------
	
	public InMem_TreeData() {
	}

	private static NodeData createEmptyRootData() {
        return new NodeData(NodeName.EMPTY,
                NodeData.TYPE_DIR, // type;
                0, // mask;
                ImmutableSet.<NodeName>of(), // childNames, 
                ImmutableMap.<String,NodeAttr>of(), // attrs
                0L, // externalCreationTime, 
                0L, // externalLastModifiedTime,
                0L, // externalLength;
                0L, // lastExternalRefreshTimeMillis
                0L, // lastTreeDataUpdateTimeMillis
                0, // lastTreeDataUpdateCount
                0, 0, 0, 0L); // treeDataRecomputationMask, lruCount, lruAmortizedCount, lastQueryTimestamp
	}
	
	// ------------------------------------------------------------------------

	@Override
	public NodeData get(NodeNamesPath path) {
		InMemNodeEntry entry = resolveUpTo(path, path.size());
		if (entry == null) {
			return null;
		}
		return entry.data;
	}

	@Override
	public NodeData getWithChild(NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		InMemNodeEntry entry = resolveUpTo(path, path.size());
		if (entry == null) {
			return null;
		}
		NodeData res = entry.data;
		if (res == null) {
			return null;
		}
		val childNames = res.childNames;
		if (childNames != null && ! childNames.isEmpty()) {
			val sortedEntries = entry.sortedEntries;
			if (sortedEntries == null) {
				notFoundChildLs.addAll(childNames);
			} else {
				for(val childName: childNames) {
					val childEntry = sortedEntries.get(childName);
					val childData = (childEntry != null)? childEntry.data : null;
					if (childData != null) {
						foundChildMap.put(childName, childData);
					} else {
						notFoundChildLs.add(childName);
					}
				}
			}
		}
		return entry.data;
	}

	
	@Override
	public void put(NodeNamesPath path, NodeData data) {
		val pathEltCount = path.size();
		InMemNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = path.get(i);
			currEntry = currEntry.getOrCreateEntry(pathElt);
		}
		currEntry.data = data;
	}
	
	@Override
	public void remove(NodeNamesPath path) {
		val parent = resolveParent(path);
		if (parent.sortedEntries == null) {
			return; // ok: no child to remove
		}
		val childName = path.last();
		parent.removeEntry(childName);
	}

	// additionnal specific 'api' (?) methods
	// ------------------------------------------------------------------------
	
	public void put_root(NodeData data) {
		rootNode.data = data;
	}
	
	public void put_strictNoCreateParent(NodeNamesPath path, NodeData data) {
		if (path == null || path.size() == 0) {
			put_root(data);
			return;
		}
		InMemNodeEntry parent = resolveParent(path);
		if (parent == null) {
			throw new IllegalArgumentException();
		}
		parent.addEntry(data);
	}

	public InMemNodeEntry resolveParent(NodeNamesPath path) {
		val parentPathEltCount = path.size() - 1;
		return resolveUpTo(path, parentPathEltCount);
	}
	
	public InMemNodeEntry resolveUpTo(NodeNamesPath path, int pathEltCount) {
		InMemNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = path.get(i);
			val foundChildEntry = currEntry.getEntry(pathElt);
			if (foundChildEntry == null) {
				return null; 
			} else {
				currEntry = foundChildEntry;
			}
		}
		return currEntry;
	}
	
	// TODO move to IndexedBlobStorage_TreeNodeDataEncoder + wrap in specific entry with 
	// ------------------------------------------------------------------------

	public void recursiveWriteFull(BlobStorage blobStorage, String fileName,			
			IndexedBlobStorage_TreeNodeDataEncoder encoder
			) {
		int writeBufferSize = 10 * 4096;
		try(val out = new BufferedOutputStream(blobStorage.openWrite(fileName, false), writeBufferSize)) {
			recursiveWriteFull(out, encoder);
		} catch (IOException ex) {
			throw new RuntimeException("Failed write to '" + fileName + "'", ex);
		}
	}

	public void recursiveWriteFull(OutputStream outputStream,
			IndexedBlobStorage_TreeNodeDataEncoder encoder
			) throws IOException {
		NoFlushCountingOutputStream countingOut = new NoFlushCountingOutputStream(outputStream); 
		DataOutputStream out = new DataOutputStream(countingOut);
		try {
			// step 1/3: recompute all dataLen
			val computeHelper = new RecomputeDataLenHelper(encoder);
			doRecursiveComputeDataLen(rootNode, computeHelper);
	
			// step 2/3: recompute all synthetized filePos
			long headerStartPos = 0L;
			doRecursiveComputeFilePos(rootNode, headerStartPos);
	
			// step 3/3: do write with filePos
			doRecursiveWriteNode(out, countingOut, encoder, rootNode);
			
		} finally {
			out.flush();
		}
	}

	// internal
	// ------------------------------------------------------------------------
	
	private void doRecursiveWriteNode(DataOutputStream out, NoFlushCountingOutputStream counting, IndexedBlobStorage_TreeNodeDataEncoder encoder,
			InMemNodeEntry node) throws IOException {
	    List<InMemNodeEntry> sortedEntries = (node.sortedEntries != null)? new ArrayList<InMemNodeEntry>(node.sortedEntries.values()) : new ArrayList<>();
        val childCount = (sortedEntries != null)? sortedEntries.size() : 0;
		long[] childDataFilePos = new long[childCount];
		for(int i = 0; i < childCount; i++) {
			InMemNodeEntry childEntry = sortedEntries.get(i);
			childDataFilePos[i] = childEntry.synthetisedDataFilePos;
		}
		
		// check computedFilePos == currentFilePos
		out.flush(); // flush DataOutputStream to get exact count... should not flush to underlying BufferedOutputStream
		val actualDataFilePos = counting.getCount();
		if (actualDataFilePos != node.synthetisedDataFilePos) {
			log.error("detected invalid file: entry " + node.name + " computed (set in parent-child ref) dataFilePos:" + node.synthetisedDataFilePos 
					+ ", but actual write at " + actualDataFilePos);
		}
		encoder.writeNodeDataAndChildIndexes(out, node.data, childDataFilePos);
		
		for(int i = 0; i < childCount; i++) {
			InMemNodeEntry childEntry = sortedEntries.get(i);
			// *** recurse ***
			doRecursiveWriteNode(out, counting, encoder, childEntry);
		}
	}
	
	private static class RecomputeDataLenHelper {
		
		private final IndexedBlobStorage_TreeNodeDataEncoder encoder;
		NullCountingOutputStream countingOutput;
		DataOutputStream out;
		private static final long[] EMPTY_LONG_ARRAY = new long[0];
		
		public RecomputeDataLenHelper(IndexedBlobStorage_TreeNodeDataEncoder encoder) {
			this.encoder = encoder;
			this.countingOutput = new NullCountingOutputStream();
			this.out = new DataOutputStream(countingOutput);
		}

		public int computeLen_nodeDataAndChildIndexes(InMemNodeEntry node) {
			countingOutput.resetCount();
			try {
				encoder.writeNodeDataAndChildIndexes(out, node.data, EMPTY_LONG_ARRAY); // => attrDataEncoderHelper.writeNodeData_noName(out, nodeData); .. no check filePos
			} catch(IOException ex) {
				throw new RuntimeException("should not occur", ex);
			}
			val dataLen = (int) countingOutput.getCount();
			int childCount = (node.sortedEntries != null)? node.sortedEntries.size() : 0;
			return dataLen + 8 * childCount;
		}

	}
	
	private void doRecursiveComputeDataLen(InMemNodeEntry node, RecomputeDataLenHelper computeHelper) {
		node.dataAndChildFilePosLen = computeHelper.computeLen_nodeDataAndChildIndexes(node);

		int childSum = 0;
		if (node.sortedEntries != null) {
			for(val child: node.sortedEntries.values()) {
				// ** recurse **
				doRecursiveComputeDataLen(child, computeHelper);
				childSum += child.recursiveDataLenSum; 
			}
		}
		node.recursiveDataLenSum = childSum;
	}

	private long doRecursiveComputeFilePos(InMemNodeEntry node, long startPos) {
		long currPos = startPos;
		node.synthetisedDataFilePos = startPos;
		currPos += node.dataAndChildFilePosLen;
		
		if (node.sortedEntries != null) {
			for(val child: node.sortedEntries.values()) {
				// ** recurse **
				currPos = doRecursiveComputeFilePos(child, currPos);
			}
		}
		return currPos;
	}

}
