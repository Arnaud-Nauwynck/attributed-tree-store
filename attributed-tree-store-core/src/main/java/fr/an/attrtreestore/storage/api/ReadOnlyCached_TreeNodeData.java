package fr.an.attrtreestore.storage.api;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

public abstract class ReadOnlyCached_TreeNodeData {

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
