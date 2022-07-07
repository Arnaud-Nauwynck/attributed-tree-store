package fr.an.attrtreestore.api;

import java.util.concurrent.CompletableFuture;

import org.path4j.NodeNamesPath;

public interface IAsyncWriteTreeData {

	// TODO ... need to provide function to fill intermediate unknown nodes..
	// public abstract CompletableFuture<Void> asyncPut_createIntermediate(NodeNamesPath path, NodeData data, Function<NodeNamesPath,NodeData> func);
	// otherwise
	// public abstract CompletableFuture<Void> asyncPut_strictNoCreateIntermediate(NodeNamesPath path, NodeData data);
	

	public CompletableFuture<Void> asyncPut(NodeNamesPath path, NodeData data);
	
	public CompletableFuture<Void> asyncRemove(NodeNamesPath path);

}
