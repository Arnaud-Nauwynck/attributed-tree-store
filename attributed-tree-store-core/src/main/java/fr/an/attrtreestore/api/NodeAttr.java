package fr.an.attrtreestore.api;

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
	
}
