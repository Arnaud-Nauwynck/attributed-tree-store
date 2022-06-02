package fr.an.attrtreestore.storage.api;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

public abstract class ROCached_TreeData implements IReadTreeData {

	public enum IndexedBlobStorageInitMode {
		RELOAD_ROOT_ONLY,
		RELOAD_FULL,
		NOT_INITIALIZED;		
	}

	protected abstract void init(IndexedBlobStorageInitMode initMode, long initPreloadSize);
	
	// @Deprecated.. shoud use async Api?
	public abstract NodeData get(NodeNamesPath path);
	
	// public abstract CompletableFuture<NodeData> asyncGet(NodeNamesPath path);

}
