package fr.an.attrtreestore.api;

import java.util.concurrent.CompletableFuture;

import org.path4j.NodeNamesPath;

public interface IAsyncReadTreeData {

	public CompletableFuture<NodeData> asyncGet(NodeNamesPath path);

}
