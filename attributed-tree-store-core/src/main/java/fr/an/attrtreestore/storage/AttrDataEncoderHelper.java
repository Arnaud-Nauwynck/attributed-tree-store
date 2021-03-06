package fr.an.attrtreestore.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.path4j.NodeName;
import org.path4j.NodeNameEncoder;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeData.NodeDataInternalFields;
import fr.an.attrtreestore.api.attrinfo.AttrEvalStatus;
import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
public class AttrDataEncoderHelper {

	public final AttrInfoIndexes attrIndexes;
	public final NodeNameEncoder nodeNameEncoder;
	
	// ------------------------------------------------------------------------
	
	public void writeNodeData_noName(DataOutput out, NodeData nodeData
			) throws IOException {
		if (nodeData == null) {
			out.writeInt(-1);
			return;
		}
		
		out.writeInt(nodeData.type);
		
		out.writeInt(nodeData.mask);

		writeIncrNodeNames(out, nodeData.childNames, "");
		
		writeAttrDatas(out, nodeData.attrs.values());

		out.writeLong(nodeData.externalCreationTime);
		out.writeLong(nodeData.externalLastModifiedTime);
		out.writeLong(nodeData.externalLength);
		out.writeLong(nodeData.getLastExternalRefreshTimeMillis());
		
		out.writeLong(nodeData.lastTreeDataUpdateTimeMillis);
		out.writeInt(nodeData.lastTreeDataUpdateCount);
		
		out.writeInt(nodeData.getTreeDataRecomputationMask());
		out.writeInt(nodeData.getLruCount());
		out.writeInt(nodeData.getLruAmortizedCount());
		
		out.writeLong(nodeData.getLastTreeDataQueryTimeMillis());
	}

	public NodeData readNodeData_noName(DataInput in, NodeName name) throws IOException {
		
		int type = in.readInt();
		if (type == -1) {
			return null;
		}
		
		int mask = in.readInt();
		
		val childNameArray = readIncrNodeNames(in, "", nodeNameEncoder);
		val childNames = ImmutableSet.<NodeName>copyOf(childNameArray);
		
		val attrs = readAttrDatas_immutableMap(in);
		
		val externalCreationTime = in.readLong();
		val externalLastModifiedTime = in.readLong();
		val externalLength = in.readLong();		
		val lastExternalRefreshTimeMillis = in.readLong();
		
		val lastTreeDataUpdateTimeMillis = in.readLong();
		val lastTreeDataUpdateCount = in.readInt();
		
		val treeDataRecomputationMask = in.readInt();
		val lruCount = in.readInt();
		val lruAmortizedCount = in.readInt();
		val lastTreeDataQueryTimeMillis = in.readLong();
		
		return new NodeData(name, type, mask, childNames, attrs, // 
				externalCreationTime, externalLastModifiedTime, externalLength, lastExternalRefreshTimeMillis, //
				lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount, //
				treeDataRecomputationMask, //
				lruCount, lruAmortizedCount, lastTreeDataQueryTimeMillis
				);
	}

	public void writeNodeData_internalFields(DataOutput out, NodeDataInternalFields src
			) throws IOException {
		out.writeLong(src.getLastExternalRefreshTimeMillis());
		out.writeInt(src.getTreeDataRecomputationMask());
		out.writeInt(src.getLruCount());
		out.writeInt(src.getLruAmortizedCount());
		out.writeLong(src.getLastTreeDataQueryTimeMillis());
	}

	public NodeDataInternalFields readNodeData_internalFields(DataInput in) throws IOException {
		val lastExternalRefreshTimeMillis = in.readLong();
		val treeDataRecomputationMask = in.readInt();
		val lruCount = in.readInt();
		val lruAmortizedCount = in.readInt();
		val lastTreeDataQueryTimeMillis = in.readLong();
		return new NodeDataInternalFields(lastExternalRefreshTimeMillis, //
				treeDataRecomputationMask, //
				lruCount, lruAmortizedCount, lastTreeDataQueryTimeMillis
				);
	}

	
	public void writeAttrDatas(DataOutput out, NodeAttr[] attrs) throws IOException {
		int attrCount = attrs.length;
		out.writeShort((short) attrCount); // might use 1 byte
		for(int i = 0; i < attrCount; i++) {
			NodeAttr attr = attrs[i];
			val attrInfo = attr.attrInfo;
			int attrIndex = attrIndexes.attrToIndex(attrInfo);
			out.writeShort((short) attrIndex); // might use 1 byte

			out.writeLong(attr.getLastEvalTimestamp());
			out.writeByte(attr.getEvalStatus().toByte());

			val attrEncoder = attrInfo.attrDataEncoder;
			attrEncoder.writeData(out, attr.getData());
		}
	}

	public void writeAttrDatas(DataOutput out, ImmutableCollection<NodeAttr> attrs) throws IOException {
		int attrCount = attrs.size();
		out.writeShort((short) attrCount); // might use 1 byte
		for(val attr: attrs) {
			val attrInfo = attr.attrInfo;
			int attrIndex = attrIndexes.attrToIndex(attrInfo);
			out.writeShort((short) attrIndex); // might use 1 byte

			out.writeLong(attr.getLastEvalTimestamp());
			out.writeByte(attr.getEvalStatus().toByte());

			val attrEncoder = attrInfo.attrDataEncoder;
			attrEncoder.writeData(out, attr.getData());
		}
	}

	public NodeAttr[] readAttrDatas(DataInput in) throws IOException {
		int attrCount = in.readShort(); // might use 1 byte
		val res = new NodeAttr[attrCount];
		for(int i = 0; i < attrCount; i++) {
			int attrIndex = in.readShort(); // might use 1 byte
			
			val attrInfo = attrIndexes.indexToAttr(attrIndex);
			
			long lastEvalTimestamp = in.readLong();
			AttrEvalStatus evalStatus = AttrEvalStatus.fromByte(in.readByte());
			
			val attrEncoder = attrInfo.attrDataEncoder;
			Object data = attrEncoder.readData(in);
			
			res[i] = new NodeAttr(attrInfo, data, lastEvalTimestamp, evalStatus);
		}
		return res;
	}

	public ImmutableMap<String,NodeAttr> readAttrDatas_immutableMap(DataInput in) throws IOException {
		NodeAttr[] attrs = readAttrDatas(in);
		val b = ImmutableMap.<String,NodeAttr>builder();
		for(val attr: attrs) {
			b.put(attr.getName(), attr);
		}
		return b.build();
	}

	public static void writeIncrString(DataOutput out, String value, String prev) throws IOException {
		int commonLen = commonStringLen(value, prev);
		int removeLen = prev.length() - commonLen;
		out.writeShort(removeLen);
		String addStr = value.substring(commonLen, value.length());
		out.writeUTF(addStr);
	}
	
	public static String readIncrString(DataInput in, String prev) throws IOException {
		int removeLen = in.readShort();
		val addStr = in.readUTF();
		int commonLen = prev.length() - removeLen;
		if (commonLen == 0) {
			return addStr;
		} else {
			return prev.subSequence(0, commonLen) + addStr;
		} 
	}

	public static void writeIncrNodeNames(DataOutput out, ImmutableSet<NodeName> nodeNames, String prev) throws IOException {
		String curr = prev;
		int valuesCount = nodeNames.size();
		out.writeInt(valuesCount); // TOADD may encode on 1 byte for < 2^7=128, 2 bytes for < 2^15=.. else 4 bytes 
		for(val nodeName: nodeNames) {
			String value = nodeName.toText();
			writeIncrString(out, value, curr);
			curr = value;
		}
	}
	
	public static NodeName[] readIncrNodeNames(DataInput in, String prev, NodeNameEncoder nodeNameEncoder) throws IOException {
		String curr = prev;
		int valuesCount = in.readInt(); // TOADD may decode on 1 byte for < 2^7=128, 2 bytes for < 2^15=.. else 4 bytes
		val res = new NodeName[valuesCount];  
		for(int i = 0; i < valuesCount; i++) {
			String value = readIncrString(in, curr);
			
// ********************* TODO TEMPORARY BUG HACK
			String name = value;
            if (name.endsWith("/")) {
                // should not occur
                name = name.substring(0, name.length()-1);
            }
            val idxSep = name.lastIndexOf("/") ;
            if (-1 != idxSep) {
                // should not occur
                name = name.substring(idxSep + 1, name.length());
            }
// ********************* 
            
			res[i] = nodeNameEncoder.encode(name);
			curr = value;
		}
		return res;
	}
	
	public static void writeIncrStrings(DataOutput out, Collection<String> values, String prev) throws IOException {
		String curr = prev;
		int valuesCount = values.size();
		out.writeInt(valuesCount); // TOADD may encode on 1 byte for < 2^7=128, 2 bytes for < 2^15=.. else 4 bytes 
		for(val value : values) {
			writeIncrString(out, value, curr);
			curr = value;
		}
	}
	
	public static String[] readIncrStrings(DataInput in, String prev) throws IOException {
		String curr = prev;
		int valuesCount = in.readInt(); // TOADD may decode on 1 byte for < 2^7=128, 2 bytes for < 2^15=.. else 4 bytes
		val res = new String[valuesCount];  
		for(int i = 0; i < valuesCount; i++) {
			String value = readIncrString(in, curr);
			res[i] = value;
			curr = value;
		}
		return res;
	}
	
	/*pp*/ static int commonStringLen(String left, String right) {
		int i = 0;
		int len = Math.min(left.length(), right.length());
		for(; i < len; i++) {
			if (left.charAt(i) != right.charAt(i)) {
				return i;
			}
		}
		return len;
	}

}
