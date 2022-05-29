package fr.an.attrtreestore.spi;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

/**
 * in-memory object for tree node with computed Attributes
 * 
 * object forms a partial Tree node hierarchy, and may be evicted from memory at any child depth.
 * objects may be reloaded from cache.
 * 
 * Any code handling Nodes must keep the full path or resolved parent path Nodes, as their is NO pointer to parent node.
 * 
 * NOTICE: this class try to be minimalist in memory footprint
 * ... BUT BEWARE there might be > 200 Millions nodes in memory
 * 
 */
public class Node {

	public static final NodeAttr[] EMPTY_ATTRS = new NodeAttr[0];

	// Node parent; // implicit..
	
	public final NodeName name;

	/** use-defined type... example: file / node / .. */
	@Getter
	protected int type;

	@Getter
	protected int mask;

	// when loaded => downcast to Node
	// when not loaded => downcast to NodeHandle (name + type)
	private Object[] sortedEntries; 
	
	@AllArgsConstructor
	private static class NodeEntryHandle {
		final NodeName name;
		long filePos;
		
	}

	
	private NodeAttr[] sortedAttrArray;

	@Getter
	protected long creationTime;
	
	@Getter
	protected long lastModifiedTime;
	
	/** use-defined field1Long: in case (frequent) this node represent a file... the fileLength */
	@Getter
	protected long field1Long;
	

	
	@Getter
	private int lruCount;
	@Getter
	private int lruAmortizedCount;

	@Getter
	private long lastQueryTimestamp;
	@Getter
	private long lastModifTimestamp;


	
	
	// ------------------------------------------------------------------------
	
	public Node(NodeName name) {
		this.name = name;
	}

	public Node(NodeName name, long creationTime, long lastModifiedTime, NodeAttr[] sortedAttrArray) {
		this.name = name;
		this.creationTime = creationTime;
		this.lastModifiedTime = lastModifiedTime;
		this.sortedAttrArray = sortedAttrArray;
	}

	// ------------------------------------------------------------------------
	
	public int getChildCount() {
		return sortedEntries.length;
	}

	public NodeName childName(int i) {
		val e = sortedEntries[i];
		return (e instanceof Node)? ((Node) e).name : ((NodeEntryHandle) e).name;
	}

	/** return array copy of childNames */
	public NodeName[] getChildNames() {
		int len = sortedEntries.length;
		val res = new NodeName[len];
		for(int i = 0; i < len; i++) {
			val e = sortedEntries[i];
			res[i] = (e instanceof Node)? ((Node) e).name : ((NodeEntryHandle) e).name;
		}
		return res;
	}
	
	public CompletableFuture<Node> asyncLoadChild(int i, NodeTreeLoader ownerLoader) {
		val e = sortedEntries[i];
		if (e instanceof Node) {
			return CompletableFuture.completedFuture((Node) e);
		}
		// cache miss .. need to async reload entry from cache
		val entry = (NodeEntryHandle) e;
		return ownerLoader.asyncLoadChild(this, entry.name, entry.filePos);
	}
	
	public CompletableFuture<Node[]> asyncLoadChildArray(NodeTreeLoader ownerLoader) {
		val childCount = sortedEntries.length;
		val res = new Node[childCount];
		CompletableFuture<Node[]> f = CompletableFuture.completedFuture(res);
		// combine load child one by one... not using combineAll (may not be faster.. file should be sequentially read by child)
		for(int i = 0; i < childCount; i++) {
			val e = sortedEntries[i];
			if (e instanceof Node) {
				res[i] = (Node) e;
			} else {
				val eh = (NodeEntryHandle) e;
				val finalI = i;
				f = f.thenCompose(x -> 
					ownerLoader.asyncLoadChild(this, eh.name, eh.filePos)
						.thenApply(childNode -> { res[finalI] = childNode; return x; })
						);
			}
		}
		return f;
	}


	// Attributes
	// ------------------------------------------------------------------------
	
	public void setNodeAttrMap(TreeMap<String,NodeAttr> attrMap) {
		this.sortedAttrArray = attrMap.values().toArray(new NodeAttr[attrMap.size()]);
	}
	
	public int getAttrCount() {
		return sortedAttrArray.length;
	}

	public NodeAttr attr(int i) {
		return sortedAttrArray[i];
	}

	public TreeMap<String,NodeAttr> getNodeAttrMapCopy() {
		val res = new TreeMap<String,NodeAttr>();
		for(val attr: sortedAttrArray) {
			res.put(attr.getName(), attr);
		}
		return res;
	}
	
	// may use ImmutableList copy?
	public NodeAttr[] _friend_getAttrs() {
		return sortedAttrArray;
	}

	// ------------------------------------------------------------------------
	
	public void setBasicFileAttrs(long creationTime, long lastModifiedTime) {
		this.creationTime = creationTime;
		this.lastModifiedTime = lastModifiedTime;
	}

	// ------------------------------------------------------------------------
	
	
	
}
