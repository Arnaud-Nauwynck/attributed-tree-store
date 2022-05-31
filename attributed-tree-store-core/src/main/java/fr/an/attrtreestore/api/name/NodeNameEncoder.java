package fr.an.attrtreestore.api.name;

import fr.an.attrtreestore.api.NodeName;
import lombok.val;

public abstract class NodeNameEncoder {

	public abstract NodeName encode(String name);	

	/**
	 * encode several names from a path
	 *  
	 * may override to decide to internalize name where path level < threshold.. 
	 */ 
	public NodeName[] encodePath(String[] pathElts) {
		val pathCount = pathElts.length;
		val res = new NodeName[pathCount];
		for(int i = 0; i < pathCount; i++) {
			res[i] = encode(pathElts[i]);
		}
		return res;
	}

}
