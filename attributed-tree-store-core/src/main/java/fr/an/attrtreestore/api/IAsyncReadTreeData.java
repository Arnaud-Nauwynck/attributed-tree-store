package fr.an.attrtreestore.api;

import java.util.concurrent.CompletableFuture;

public interface IAsyncReadTreeData {

	public CompletableFuture<NodeData> asyncGet(NodeNamesPath path);

}
