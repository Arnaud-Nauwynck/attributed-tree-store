package fr.an.attrtreestore.storage.api;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.TreeMap;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FullInMem_TreeNodeData {

	private final FullInMemNodeEntry rootNode = new FullInMemNodeEntry(null, null, 0, 0L, 0L, new TreeMap<>());

	@AllArgsConstructor
	private static class FullInMemNodeEntry {
		@Getter
		final NodeName name;
		
		NodeData data;
		
		// computed from recursive data encoding
		int dataAndChildFilePosLen; // computed from data
		long recursiveDataLenSum; // computed from dataAndChildFilePosLen + recursively scan on sortedEntries 
		long synthetisedDataFilePos; 

		TreeMap<NodeName,FullInMemNodeEntry> sortedEntries;
		
		FullInMemNodeEntry getChild(NodeName chldName) {
			if (sortedEntries == null) {
				return null;
			}
			return sortedEntries.get(chldName);
		}

		@Override
		public String toString() {
			return "FullInMemNodeEntry [" + name 
					+ ((sortedEntries != null && sortedEntries.size() != 0)? ", " + sortedEntries.size() + " child" : "")
					+ "]";
		}
		
		
	}
	
	// ------------------------------------------------------------------------
	
	public FullInMem_TreeNodeData() {
	}

	// ------------------------------------------------------------------------

	public void put_root(NodeData data) {
		rootNode.data = data;
	}
	
	public void put_strictNoCreateParent(NodeNamesPath path, NodeData data) {
		if (path == null || path.pathElements.length == 0) {
			put_root(data);
			return;
		}
		FullInMemNodeEntry parent = resolveParent(path);
		if (parent == null) {
			throw new IllegalArgumentException();
		}
		val childName = path.pathElements[path.pathElements.length - 1];
		val foundChildEntry = parent.getChild(childName);
		if (foundChildEntry != null) {
			// update data..
			foundChildEntry.data = data;
			
			// TOADD... check remove child if not present in data.childNames
		} else {
			if (parent.sortedEntries == null) {
				parent.sortedEntries = new TreeMap<>();
			}
			val newChildEntry = new FullInMemNodeEntry(childName, data, 0, 0L, 0L, null);
			parent.sortedEntries.put(childName, newChildEntry);
		}
	}
	
	public void put(NodeNamesPath path, NodeData data) {
		val pathElts = path.pathElements;
		val pathEltCount = pathElts.length;
		FullInMemNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			if (currEntry.sortedEntries == null) {
				currEntry.sortedEntries = new TreeMap<>(); 
			}
			val pathElt = pathElts[i];
			val foundChildEntry = currEntry.sortedEntries.get(pathElt);
			if (foundChildEntry == null) {
				// if allowAutoCreateParent... 
				val newChildEntry = new FullInMemNodeEntry(pathElt, null, 0, 0L, 0L, null);
				currEntry.sortedEntries.put(pathElt, newChildEntry);
				currEntry = newChildEntry;
			} else {
				currEntry = foundChildEntry;
			}
		}
		currEntry.data = data;
	}
	
	public void remove(NodeNamesPath path) {
		val parent = resolveParent(path);
		if (parent.sortedEntries == null) {
			return; // ok: no child to remove
		}
		val removed = parent.sortedEntries.remove(path);
		if (removed == null) {
			// no child removed
		} else {
			// TOADD recursive mark child as deleted.. maybe help GC
		}
	}

	public FullInMemNodeEntry resolveParent(NodeNamesPath path) {
		val parentPathEltCount = path.pathElements.length - 1;
		return resolveUpTo(path, parentPathEltCount);
	}
	
	public FullInMemNodeEntry resolveUpTo(NodeNamesPath path, int pathEltCount) {
		val pathElts = path.pathElements;
		FullInMemNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			val foundChildEntry = currEntry.getChild(pathElt);
			if (foundChildEntry == null) {
				return null; 
			} else {
				currEntry = foundChildEntry;
			}
		}
		return currEntry;
	}
	
	public NodeData get(NodeNamesPath path) {
		FullInMemNodeEntry entry = resolveUpTo(path, path.pathElements.length);
		if (entry == null) {
			return null;
		}
		return entry.data;
	}

	// ------------------------------------------------------------------------

	public void recursiveWriteFull(BlobStorage blobStorage, String fileName,			
			IndexedBlobStorage_TreeNodeDataEncoder encoder) {
		int writeBufferSize = 10 * 4096;
		try(val out = new BufferedOutputStream(blobStorage.openWrite(fileName, false), writeBufferSize)) {
			recursiveWriteFull(out, encoder);
		} catch (IOException ex) {
			throw new RuntimeException("Failed write to '" + fileName + "'", ex);
		}
	}

	public void recursiveWriteFull(OutputStream outputStream,
			IndexedBlobStorage_TreeNodeDataEncoder encoder) throws IOException {
		NoFlushCountingOutputStream countingOut = new NoFlushCountingOutputStream(outputStream); 
		DataOutputStream out = new DataOutputStream(countingOut);
		try {
			// step 1/3: recompute all dataLen
			val computeHelper = new RecomputeDataLenHelper(encoder);
			doRecursiveComputeDataLen(rootNode, computeHelper);
	
			// step 2/3: recompute all synthetized filePos
			long headerStartPos = 0L;
			doRecursiveComputeFilePos(rootNode, headerStartPos);
	
			// step 2/3: do write with filePos
			doRecursiveWriteNode(out, countingOut, encoder, rootNode);
			
		} finally {
			out.flush();
		}
	}

	private void doRecursiveWriteNode(DataOutputStream out, NoFlushCountingOutputStream counting, IndexedBlobStorage_TreeNodeDataEncoder encoder,
			FullInMemNodeEntry node) throws IOException {
		val childNames = new ArrayList<>(node.data.childNames); // TOCHECK ensure sorted
		val childCount = childNames.size();
		long[] childDataFilePos = new long[childCount];
		for(int i = 0; i < childCount; i++) {
			val childName = childNames.get(i);
			FullInMemNodeEntry childEntry = node.sortedEntries.get(childName);
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
			val childName = childNames.get(i);
			FullInMemNodeEntry childEntry = node.sortedEntries.get(childName);
			// *** recurse ***
			doRecursiveWriteNode(out, counting, encoder, childEntry);
		}
	}
	
	private static final class NullCountingOutputStream extends OutputStream {

		private long count;

		public void resetCount() {
			this.count = 0;
		}
		public long getCount() {
			return count;
		}

		@Override
		public void write(byte[] b, int off, int len) {
			count += len;
		}

		@Override
		public void write(int b) {
			count++;
		}

		@Override
		public void close() {
		}
	}
	
	public final class NoFlushCountingOutputStream extends FilterOutputStream {

		private long count;

		public NoFlushCountingOutputStream(OutputStream out) {
		    super(out);
		  }

		public long getCount() {
			return count;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			out.write(b, off, len);
			count += len;
		}

		@Override
		public void write(int b) throws IOException {
			out.write(b);
			count++;
		}

		@Override
		public void flush() throws IOException {
			// override to avoid propagation to underlying flush !
		}

		@Override
		public void close() throws IOException {
			out.close();
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

		public int computeLen_nodeDataAndChildIndexes(FullInMemNodeEntry node) {
			countingOutput.resetCount();
			try {
				encoder.writeNodeDataAndChildIndexes(out, node.data, EMPTY_LONG_ARRAY); // => attrDataEncoderHelper.writeNodeData_noName(out, nodeData); .. no check filePos
			} catch(IOException ex) {
				throw new RuntimeException("should not occur", ex);
			}
			val dataLen = (int) countingOutput.getCount();
			return dataLen + 8 * node.data.childNames.size();
		}

	}
	
	private void doRecursiveComputeDataLen(FullInMemNodeEntry node, RecomputeDataLenHelper computeHelper) {
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

	private long doRecursiveComputeFilePos(FullInMemNodeEntry node, long startPos) {
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
