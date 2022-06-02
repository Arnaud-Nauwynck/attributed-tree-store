package fr.an.attrtreestore.storage.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * IO helper TreeData
 * delegating to <code>AttrDataEncoderHelper attrDataEncoderHelper</code>
 * .. which itself delegates to
 * 	  <code>AttrInfoIndexes attrIndexes</code>
 *    <code>NodeNameEncoder nodeNameEncoder</code>
 *
 */
@Slf4j
public class IndexedBlobStorage_TreeNodeDataEncoder {

	//	private static final String FILE_HEADER = "readonly-full-tree-data";

	public static final long FIXED_ROOT_FILEPOS = 0;
	
	protected final AttrDataEncoderHelper attrDataEncoderHelper;

	// ------------------------------------------------------------------------
	
	public IndexedBlobStorage_TreeNodeDataEncoder(
			AttrDataEncoderHelper attrDataEncoderHelper) {
		this.attrDataEncoderHelper = attrDataEncoderHelper;
	}
	
	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class NodeDataAndChildFilePos {
		public final NodeData nodeData;
		public final long[] childDataFilePos;
	}
	
	public NodeDataAndChildFilePos readNodeDataAndChildIndexes(DataInputStream in, NodeName name) throws IOException {
		val nodeData = attrDataEncoderHelper.readNodeData_noName(in, name);
		val childCount = nodeData.childNames.size();
		long[] childDataFilePos = new long[childCount];
		for(int i = 0; i < childCount; i++) {
			childDataFilePos[i] = in.readLong();
		}
		val checkEnd = in.read(); // useless but easier for read/debug/check
		if (checkEnd != '\n') {
			log.error("expecting '\n', got " + checkEnd);
		}
		return new NodeDataAndChildFilePos(nodeData, childDataFilePos);
	}
	
	public void writeNodeDataAndChildIndexes(DataOutputStream out, NodeData nodeData, long[] childDataFilePos) throws IOException {
		attrDataEncoderHelper.writeNodeData_noName(out, nodeData);
		val childCount = childDataFilePos.length;
		for(int i = 0; i < childCount; i++) {
			out.writeLong(childDataFilePos[i]);
		}
		out.write('\n'); // useless but easier for read/debug/check
	}
	
}
