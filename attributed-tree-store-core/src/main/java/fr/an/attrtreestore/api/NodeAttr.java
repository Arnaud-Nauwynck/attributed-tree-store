package fr.an.attrtreestore.api;

import java.util.Objects;

import fr.an.attrtreestore.api.attrinfo.AttrEvalStatus;
import fr.an.attrtreestore.api.attrinfo.AttrInfo;
import lombok.Getter;

@Getter
public class NodeAttr {

	public final AttrInfo<Object> attrInfo;
	
	private Object data;

	private long lastEvalTimestamp;

	private AttrEvalStatus evalStatus;
	
	// ------------------------------------------------------------------------

	public NodeAttr(AttrInfo<Object> attrInfo, Object data, long lastEvalTimestamp, AttrEvalStatus evalStatus) {
		this.attrInfo = attrInfo;
		this.data = data;
		this.lastEvalTimestamp = lastEvalTimestamp;
		this.evalStatus = evalStatus;
	}

	// ------------------------------------------------------------------------
	
	public String getName() {
		return attrInfo.name;
	}

	@Override
	public int hashCode() {
		return Objects.hash(attrInfo, data, evalStatus, lastEvalTimestamp);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeAttr other = (NodeAttr) obj;
		return Objects.equals(attrInfo, other.attrInfo) 
				&& Objects.equals(data, other.data)
				&& evalStatus == other.evalStatus 
				// && lastEvalTimestamp == other.lastEvalTimestamp .. ignore field
				;
	}
	
	
}
