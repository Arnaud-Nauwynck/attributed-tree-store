package fr.an.attrtreestore.api;

import org.path4j.NodeNamesPath;

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
	
}
