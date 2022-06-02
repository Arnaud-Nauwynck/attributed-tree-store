package fr.an.attrtreestore.api;

public abstract class TreeData implements IReadTreeData { //, IWriteTreeData

	@Override
	public abstract NodeData get(NodeNamesPath path);
	
	// public abstract CompletableFuture<NodeData> asyncGet(NodeNamesPath path);

	
	// ------------------------------------------------------------------------
	
	// methods from IWriteTreeData, but should not implements IWriteTreeData by default
	// pre-declared, but as protected for jvm optim (miranda methods) ??
	
	protected abstract void put(NodeNamesPath path, NodeData data);
	
	protected abstract void remove(NodeNamesPath path);
	
}
