package fr.an.attrtreestore.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.storage.NodeTreeLoader;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class Node {

	private static final Object[] EMPTY_CHILD = new Object[0];
	
//	private static final Node[] EMPTY_CHILD_NODES = new Node[0];
//	private static final NodeName[] EMPTY_CHILD_NAMES = new NodeName[0];

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
		this.sortedEntries = EMPTY_CHILD;
		this.sortedAttrArray = EMPTY_ATTRS;
	}

	private Node(NodeName name, int type, long creationTime, long lastModifiedTime, NodeAttr[] sortedAttrArray,
			Object[] sortedEntries) {
		this.name = name;
		this.type = type;
		this.sortedEntries = sortedEntries;
		this.sortedAttrArray = sortedAttrArray;
		this.creationTime = creationTime;
		this.lastModifiedTime = lastModifiedTime;
	}

	// constructor when building from exhaustive tree (in-memory)
	public static Node create(NodeName name, int type, long creationTime, long lastModifiedTime, NodeAttr[] sortedAttrArray, //
			TreeMap<String,Node> sortedChildMap) {
		Object[] sortedEntries;
		if (sortedChildMap.isEmpty()) {
			sortedEntries = EMPTY_CHILD;
		} else {
			val childCount = sortedChildMap.size();
			val nodeArray = new Object[childCount];
			int i = 0;
			for(val child: sortedChildMap.values()) {
				nodeArray[i] = child;
				i++;
			}
			sortedEntries = nodeArray;
		}
		return new Node(name, type, creationTime, lastModifiedTime, sortedAttrArray, sortedEntries);
	}

	// constructor when reading from file
	public static Node createFromFile(NodeName name, int type, long creationTime, long lastModifiedTime, NodeAttr[] sortedAttrArray, //
			TreeMap<NodeName,Long> sortedChildNameAndFilePos) {
		Object[] sortedEntries;
		val childCount = (sortedChildNameAndFilePos != null)? sortedChildNameAndFilePos.size() : 0;
		if (childCount == 0) {
			sortedEntries = EMPTY_CHILD;
		} else {
			val nodeArray = new Object[childCount];
			int i = 0;
			for(val e : sortedChildNameAndFilePos.entrySet()) {
				val childName = e.getKey();
				val filePos = e.getValue();
				nodeArray[i] = new NodeEntryHandle(childName, filePos);
			}
			sortedEntries = nodeArray;
		}
		return new Node(name, type, creationTime, lastModifiedTime, sortedAttrArray, sortedEntries);
	}

	// ------------------------------------------------------------------------
	
	public int getChildCount() {
		return sortedEntries.length;
	}

	public int findChildIndex(NodeName childName) {
		// dichotomy search child index by name
		// cf Arrays.binarySearch
		val childArray = sortedEntries; 
        int low = 0;
        int high = childArray.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Object midVal = childArray[mid];
            NodeName midName = (midVal instanceof Node)? ((Node) midVal).name : ((NodeEntryHandle) midVal).name;
            int cmp = midName.compareTo(childName);            
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
            	return mid;
            }
        }
        return -(low + 1);  // key not found.
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
	
	public void setBasicAttrs(long creationTime, long lastModifiedTime, long field1Long) {
		this.creationTime = creationTime;
		this.lastModifiedTime = lastModifiedTime;
		this.field1Long = field1Long;
	}

	// Child Entries modifications
	// ------------------------------------------------------------------------
	
	public synchronized void removeChild(NodeName childName) {
		val prevSortedEntries = sortedEntries; 
		val prevChildCount = prevSortedEntries.length;
		int idx = findChildIndex(childName);
		if (idx < 0) {
			log.warn("child '" + childName + "' not found to remove .. do nothing");
			return;
		}
		val newChildCount = prevChildCount-1;
		val newSortedEntries = new Object[newChildCount];
		if (idx > 0) {
			System.arraycopy(prevSortedEntries, 0, newSortedEntries, 0, idx);
		}
		if (idx < newChildCount) {
			System.arraycopy(prevSortedEntries, idx+1, newSortedEntries, idx, newChildCount-idx);
		}
		this.sortedEntries = newSortedEntries;
	}

	public synchronized int addChild(NodeName childName) {
		val prevSortedEntries = sortedEntries; 
		val prevChildCount = prevSortedEntries.length;
		int foundIdx = findChildIndex(childName);
		int idx;
		if (foundIdx >= 0) {
			idx = foundIdx;
			log.warn("child '" + childName + "' alreay found at index " + idx + " .. nothing to add");
		} else {
			idx = -foundIdx - 1;
			val newChildCount = prevChildCount+1;
			val newSortedEntries = new Object[newChildCount];
			if (idx > 0) {
				System.arraycopy(prevSortedEntries, 0, newSortedEntries, 0, idx);
			}
			newSortedEntries[idx] = new NodeEntryHandle(childName, 0); // no known filePos here
			if (idx+1 < newChildCount) {
				System.arraycopy(prevSortedEntries, idx, newSortedEntries, idx+1, newChildCount-idx); // TOCHECK
			}
			this.sortedEntries = newSortedEntries;
		}
		return idx;
	}

	public synchronized int addChild(Node child) {
		val prevSortedEntries = sortedEntries; 
		val prevChildCount = prevSortedEntries.length;
		val childName = child.name;
		int foundIdx = findChildIndex(childName);
		int idx;
		if (foundIdx >= 0) {
			idx = foundIdx;
			log.warn("child '" + childName + "' alreay found at index " + idx + " .. update");
			sortedEntries[idx] = child;
		} else {
			idx = -foundIdx - 1;
			val newChildCount = prevChildCount+1;
			val newSortedEntries = new Object[newChildCount];
			if (idx > 0) {
				System.arraycopy(prevSortedEntries, 0, newSortedEntries, 0, idx);
			}
			newSortedEntries[idx] = child;
			if (idx+1 < newChildCount) {
				System.arraycopy(prevSortedEntries, idx, newSortedEntries, idx+1, newChildCount-idx); // TOCHECK
			}
			this.sortedEntries = newSortedEntries;
		}
		return idx;
	}

	
	public synchronized int updateChild(Node child) {
		val prevSortedEntries = sortedEntries; 
		val prevChildCount = prevSortedEntries.length;
		val childName = child.name;
		int foundIdx = findChildIndex(childName);
		int idx;
		if (foundIdx >= 0) {
			idx = foundIdx;
			sortedEntries[foundIdx] = child;
		} else {
			idx = -foundIdx - 1;
			log.warn("update child '" + childName + "' not found ?? add");
			val newChildCount = prevChildCount+1;
			val newSortedEntries = new Object[newChildCount];
			if (idx > 0) {
				System.arraycopy(prevSortedEntries, 0, newSortedEntries, 0, idx);
			}
			newSortedEntries[idx] = child;
			if (idx+1 < newChildCount) {
				System.arraycopy(prevSortedEntries, idx, newSortedEntries, idx+1, newChildCount-idx); // TOCHECK
			}
			this.sortedEntries = newSortedEntries;
		}
		return idx;
	}

	
	@AllArgsConstructor
	public static class ChildEntriesChanged {
		public List<NodeName> removed;
		public List<NodeName> added;
	}

	public synchronized ChildEntriesChanged updateChildEntries(TreeSet<NodeName> sortedChildNameSet) {
		val removed = new ArrayList<NodeName>();
		val added = new ArrayList<NodeName>();
		// compare newSortedChildNames <-> this.sortedChildEntries ... perform add/remove
		// both sorted => use faster algo to compare..

		val prevSortedEntries = sortedEntries; 
		val prevChildCount = prevSortedEntries.length;
		
		List<NodeName> sortedChildNames = new ArrayList<>(sortedChildNameSet);
		val srcEntriesCount = sortedChildNames.size();

		val newSortedEntries = new Object[srcEntriesCount];

		int prevNodeIdx = 0;
		int srcIdx = 0;
		while(prevNodeIdx < prevChildCount && srcIdx < srcEntriesCount) {
			val srcName = sortedChildNames.get(srcIdx);
			val e = prevSortedEntries[prevNodeIdx];
			val entryName = (e instanceof Node)? ((Node) e).name : ((NodeEntryHandle) e).name;
			val cmpEntryName = entryName.compareTo(srcName);
			if (0 == cmpEntryName) {
				// ok same
				newSortedEntries[srcIdx] = e;
				// advance both
				prevNodeIdx++;
				srcIdx++;
			} else if (cmpEntryName < 0) {
				// remove entry
				removed.add(entryName);
				prevNodeIdx++;
			} else { // cmpEntryName > 0
				// add new data
				newSortedEntries[srcIdx] = new NodeEntryHandle(srcName, 0);
				added.add(srcName);
				srcIdx++;
			}
		}
		while(prevNodeIdx < prevChildCount) {
			// remove entry
			val e = prevSortedEntries[prevNodeIdx];
			val entryName = (e instanceof Node)? ((Node) e).name : ((NodeEntryHandle) e).name;
			removed.add(entryName);
			prevNodeIdx++;
		}
		while(srcIdx < srcEntriesCount) {
			// add new data
			val srcName = sortedChildNames.get(srcIdx);
			newSortedEntries[srcIdx] = new NodeEntryHandle(srcName, 0);
			added.add(srcName);
			srcIdx++;
		}
		if (removed.isEmpty() && added.isEmpty()) {
			return null; // no change
		} else {
			// change ref (synchronized?)
			this.sortedEntries = newSortedEntries;
			
			return new ChildEntriesChanged(removed, added);
		}
	}

	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return name.toText() + " (type: " + type //
				+ ((sortedEntries.length > 0)? " " +sortedEntries.length + " child" : "")
				+ ")";
	}

}
