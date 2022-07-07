package fr.an.attrtreestore.api;

import org.path4j.NodeNamesPath;

public abstract class ROCached_TreeData extends TreeData implements IReadTreeData {

	public enum IndexedBlobStorageInitMode {
		RELOAD_ROOT_ONLY,
		RELOAD_FULL,
		INIT_EMPTY,		
		NOT_INITIALIZED;		
	}

	protected abstract void init(IndexedBlobStorageInitMode initMode, long initPreloadSize);
	
	// @Deprecated.. may return CachedNodeData { data, overrideMode=DELETED,UPDATED,NOT_IN_CACHE, cachedTime } 
	// may also use async Api?
	public abstract NodeData get(NodeNamesPath path);
	
	// public abstract CompletableFuture<NodeData> asyncGet(NodeNamesPath path);


	// force Read-Only final methods by throwing..
	// ------------------------------------------------------------------------
	
	protected final void put(NodeNamesPath path, NodeData data) {
		throw new UnsupportedOperationException("method from IWritableTreeData should not be used here");
	}
	
	protected void remove(NodeNamesPath path) {
		throw new UnsupportedOperationException("method from IWritableTreeData should not be used here");
	}
}
