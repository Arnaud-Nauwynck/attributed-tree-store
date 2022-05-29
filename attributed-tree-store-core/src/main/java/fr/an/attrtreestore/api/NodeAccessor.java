package fr.an.attrtreestore.api;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import fr.an.attrtreestore.spi.Node;
import lombok.val;

/**
 * Accessor for a Node and its parents path
 * 
 * ... from off-heap cache(miss) or from memory(hit)
 * 
 */
public class NodeAccessor {

	private final NodeTree nodeTree;

	// redundant with pathNodes[pathEltCount-1]
	private Node node;
	
	// elements from array can be shared, so are considered immutable
	// array may also contains more element than pathEltCount (for sharing array between parent-child)
	// private NodeName[] pathElts;
	
	private int pathEltCount;
	
	private Node[] pathNodes;
	
	// ------------------------------------------------------------------------
	
	public NodeAccessor(NodeTree nodeTree, Node[] pathNodes, int pathEltCount) {
		this.nodeTree = nodeTree;
		this.pathEltCount = pathEltCount;
		this.pathNodes = pathNodes;
		this.node = pathNodes[pathEltCount-1];
	}

	// (parent) path elements
	// ------------------------------------------------------------------------
	
	public int getPathEltCount() {
		return pathEltCount;
	}

	public NodeName pathEltAt(int i) {
		if (i >= pathEltCount) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return pathNodes[i].name;
	}

	public void navigateToParent() {
		if (pathEltCount > 0) {
			this.pathEltCount--;
			this.node = pathNodes[pathEltCount-1];
		}
	}

	// child entries
	// ------------------------------------------------------------------------
	
	public int getChildCount() {
		return node.getChildCount();
	}

	public CompletableFuture<NodeAccessor> asyncLoadChild(int i) {
		CompletableFuture<Node> fn = node.asyncLoadChild(i, nodeTree.nodeTreeLoader());
		return fn.thenApply(childNode -> openChild(childNode));
	}

	public CompletableFuture<NodeAccessor> asyncNavigateToChild(int i) {
		CompletableFuture<Node> fn = node.asyncLoadChild(i, nodeTree.nodeTreeLoader());
		return fn.thenApply(childNode -> {
			navigateToChild(childNode);
			return this;
		});
	}

	/*pp*/  NodeAccessor openChild(Node child) {
		val childEltCount = pathEltCount + 1;
		val childPathNodes = new Node[childEltCount];
		System.arraycopy(pathNodes, 0, childPathNodes, 0, pathEltCount);
		childPathNodes[pathEltCount] = child;
		return new NodeAccessor(nodeTree, childPathNodes, childEltCount);
	}

	void navigateToChild(Node child) {
		val childCount = pathEltCount + 1;
		val childPathNodes = new Node[childCount];
		System.arraycopy(pathNodes, 0, childPathNodes, 0, pathEltCount);
		childPathNodes[pathEltCount] = child;
		this.pathNodes = childPathNodes;
		this.pathEltCount = childCount;
	}

	public CompletableFuture<NodeAccessor[]> asyncLoadChildArray() {
		CompletableFuture<Node[]> fn = node.asyncLoadChildArray(nodeTree.nodeTreeLoader());
		return fn.thenApply(childNodeArray -> openChildArray(childNodeArray));
	}

	/*pp*/ NodeAccessor[] openChildArray(Node[] childArray) {
		val childCount = pathEltCount + 1;
		val res = new NodeAccessor[childCount];
		for(int i = 0; i < childCount; i++) {
			res[i] = openChild(childArray[i]);
		}
		return res;
	}

	
	// Node Attribute getter
	// ------------------------------------------------------------------------
	
	public int getAttrCount() {
		return node.getAttrCount();
	}

	public NodeAttr attr(int i) {
		return node.attr(i);
	}
	
	public TreeMap<String,NodeAttr> getNodeAttrMapCopy() {
		return node.getNodeAttrMapCopy();
	}

}
