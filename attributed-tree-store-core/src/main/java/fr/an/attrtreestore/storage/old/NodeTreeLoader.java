package fr.an.attrtreestore.storage.old;

import java.util.concurrent.CompletableFuture;

import fr.an.attrtreestore.api.NodeName;

public abstract class NodeTreeLoader {

	public abstract CompletableFuture<Node> asyncLoadChild(Node parent, NodeName childName, long optFilePos);

}
