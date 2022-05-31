package fr.an.attrtreestore.storage;

import java.util.concurrent.CompletableFuture;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.impl.Node;

public abstract class NodeTreeLoader {

	public abstract CompletableFuture<Node> asyncLoadChild(Node parent, NodeName childName, long optFilePos);

}
