package fr.an.attrtreestore.api;

import java.util.List;
import java.util.Map;

import lombok.val;

public abstract class TreeData implements IReadTreeData { //, IWriteTreeData

	@Override
	public abstract NodeData get(NodeNamesPath path);
	
	// public abstract CompletableFuture<NodeData> asyncGet(NodeNamesPath path);

	
	// ------------------------------------------------------------------------
	
//	// methods from IWriteTreeData, but should not implements IWriteTreeData by default
//	// pre-declared, for jvm optim (miranda methods) ??
//	
//	public abstract void put(NodeNamesPath path, NodeData data);
//	
//	public abstract void remove(NodeNamesPath path);
	
	
	// should override wherever possible for optims
	@Override
	public NodeData getWithChild(NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		return defaultGetWithChild(path, foundChildMap, notFoundChildLs);
	}

	protected NodeData defaultGetWithChild(NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		NodeData res = get(path);
		if (res == null) {
			return null;
		}
		val childNames = res.childNames;
		if (childNames != null && ! childNames.isEmpty()) {
			// may add .. foundChildLs.ensureCapacity(Math.min())
			for(val childName: childNames) {
				val childPath = path.toChild(childName); 
				val childData = get(childPath);
				if (childData != null) {
					foundChildMap.put(childName, childData);
				} else {
					notFoundChildLs.add(childName);
				}
			}
		}		
		return res;
	}


}
