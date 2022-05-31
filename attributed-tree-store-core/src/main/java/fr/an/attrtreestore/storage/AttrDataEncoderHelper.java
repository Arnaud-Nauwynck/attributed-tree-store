package fr.an.attrtreestore.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.attrinfo.AttrEvalStatus;
import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
public class AttrDataEncoderHelper {

	public final AttrInfoIndexes attrIndexes;
	
	// ------------------------------------------------------------------------
	
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

	protected static int commonStringLen(String left, String right) {
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
