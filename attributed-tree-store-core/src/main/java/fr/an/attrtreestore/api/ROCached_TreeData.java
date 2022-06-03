package fr.an.attrtreestore.api;

public abstract class ROCached_TreeData extends TreeData implements IReadTreeData {

	public enum IndexedBlobStorageInitMode {
		RELOAD_ROOT_ONLY,
		RELOAD_FULL,
		NOT_INITIALIZED;		
	}

	protected abstract void init(IndexedBlobStorageInitMode initMode, long initPreloadSize);
	
	// @Deprecated.. shoud use async Api?
	public abstract NodeData get(NodeNamesPath path);
	
	// public abstract CompletableFuture<NodeData> asyncGet(NodeNamesPath path);


	// override TreeData protected write methods.. force Read-Only by throwing
	// ------------------------------------------------------------------------
	
	@Override
	protected final void put(NodeNamesPath path, NodeData data) {
		throw new UnsupportedOperationException("method from IWritableTreeData should not be used here");
	}
	
	@Override
	protected void remove(NodeNamesPath path) {
		throw new UnsupportedOperationException("method from IWritableTreeData should not be used here");
	}
}
