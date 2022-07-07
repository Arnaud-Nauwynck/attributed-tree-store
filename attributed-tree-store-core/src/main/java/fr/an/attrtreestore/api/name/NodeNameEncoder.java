package fr.an.attrtreestore.api.name;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.val;

public abstract class NodeNameEncoder {

	public abstract NodeName encode(String name);	

	/**
	 * encode several names from a path
	 *  
	 * may override to decide to internalize name where path level < threshold.. 
	 */ 
	public NodeName[] encodePathNodeNames(String[] pathElts) {
		val pathCount = pathElts.length;
		val res = new NodeName[pathCount];
		for(int i = 0; i < pathCount; i++) {
			res[i] = encode(pathElts[i]);
		}
		return res;
	}

	public NodeNamesPath encodePath(String path) {
	    if (path == null || path.equals("") || path.equals("/")) {
	        return NodeNamesPath.ROOT; 
	    }
		// TOCHECK remove first "/" if any ??
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		val pathElts = path.split("/");
		return encodePath(pathElts);
	}

	public NodeNamesPath encodePath(String[] pathElts) {
		val pathNames = encodePathNodeNames(pathElts);
		return new NodeNamesPath(pathNames);
	}

}
