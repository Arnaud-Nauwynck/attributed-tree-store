package fr.an.attrtreestore.storage.impl;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.partial.PartialNodeData;
import fr.an.attrtreestore.api.partial.PartialTreeData;
import lombok.val;

/**
 * in-memory implementation of PartialTreeData
 */
public class InMem_PartialTreeData extends PartialTreeData implements IWriteTreeData {

	private final PartialNodeEntry rootNode = new PartialNodeEntry(null, null, false, null, null);

	private static class PartialNodeEntry {

		final NodeName name;
		
		NodeData dataIfPresent;

		boolean isCompleteChildNames;

		TreeMap<NodeName,PartialNodeEntry> childEntries;

		// marker when data is absent, and some child are known to be deleted
		Set<NodeName> childNamesKnownRemoved;
		

		public PartialNodeEntry(NodeName name, NodeData dataIfPresent,
				boolean isCompleteChildNames,
				TreeMap<NodeName, PartialNodeEntry> childEntries,
				Set<NodeName> childNamesKnownRemoved) {
			this.name = name;
			this.dataIfPresent = dataIfPresent;
			this.childEntries = childEntries;
			this.childNamesKnownRemoved = childNamesKnownRemoved;
		}
		
		PartialNodeEntry getChild(NodeName chldName) {
			if (childEntries == null) {
				return null;
			}
			return childEntries.get(chldName);
		}

		@Override
		public String toString() {
			return "PartialNodeEntry [" + name
					+ ((dataIfPresent == null)? "!data" : "")
					+ ((!isCompleteChildNames)? " !completeChildNames" : "")
					// + ((!isCompleteChildNames && childNamesPresent != null)? " childNamesPresent:[" + childNamesPresent + "]": "") 
					+ "]";
		}
		
	}
	
	// ------------------------------------------------------------------------
	
	public InMem_PartialTreeData() {
	}

	// ------------------------------------------------------------------------

	@Override // NOT relevant here.. simply throw ex
	public NodeData get(NodeNamesPath path) {
		throw new IllegalStateException("should not call (from IReadTree interface)");
	}

	@Override
	public PartialNodeData getPartial(NodeNamesPath path) {
		PartialNodeEntry entry = resolveOrNull(path);
		if (entry == null) {
			return null;
		}
		NodeData dataIfPresent = entry.dataIfPresent;
		boolean isCompleteChildNames = entry.isCompleteChildNames;
		Set<NodeName> childNamesPresent = (entry.childEntries != null)? new TreeSet<>(entry.childEntries.keySet()) : null;
		Set<NodeName> childNamesKnownRemoved = (entry.childNamesKnownRemoved != null)? new TreeSet<>(entry.childNamesKnownRemoved) : null;
		
		return new PartialNodeData(dataIfPresent, isCompleteChildNames, childNamesPresent, childNamesKnownRemoved);
	}

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		PartialNodeEntry parent = resolveMkdirUpToParent(path);
		if (parent.childEntries == null) {
			parent.childEntries = new TreeMap<>();
		}
		val name = path.lastName();
		val node = parent.getChild(name);
		if (node == null) {
			// create+add node
			val newNode = new PartialNodeEntry(name, data, false, new TreeMap<>(), null);
			parent.childEntries.put(name, newNode);
		} else {
			// update node data
			node.dataIfPresent = data;
			node.isCompleteChildNames = containsAllEntries(node.childEntries, data.childNames);
			
			if (parent.childNamesKnownRemoved != null) {
				parent.childNamesKnownRemoved.remove(name);
			}
		}
	}
	
	@Override
	public void remove(NodeNamesPath path) {
		val parent = resolveMkdirUpToParent(path);
		if (parent.childEntries == null) {
			return; // ok: no child to remove
		}
		val childName = path.lastName();

		if (parent.dataIfPresent != null && parent.dataIfPresent.childNames != null 
				&& parent.dataIfPresent.childNames.contains(childName)
				) {
			// TOCHECK... parent data not in-sync with child node ...
			if (parent.childNamesKnownRemoved == null) {
				parent.childNamesKnownRemoved = new TreeSet<>();
			}
			parent.childNamesKnownRemoved.add(childName);
		}
		
		val removed = parent.childEntries.remove(childName);
		if (removed == null) {
			// no child removed
		} else {
			// TOADD recursive mark child as deleted.. maybe help GC
			
		}
		
		// recompute now if parentEntry has completeChild complete
		if (parent.dataIfPresent != null && !parent.isCompleteChildNames) {
			// check containAlls if (parent.childEntries.con)
			parent.isCompleteChildNames = containsAllEntries(parent.childEntries, parent.dataIfPresent.childNames);
		}
	}

	protected static boolean containsAllEntries(Map<NodeName,?> entries, Set<NodeName> keys) {
		if (entries == null || entries.size() < keys.size()) {
			return false;
		}
		for(val key: keys) {
			if (! entries.containsKey(key)) {
				return false;
			}
		}
		return true;
	}
	
	protected void refreshChildNamesKnownRemoved(PartialNodeEntry parent) {
		if (parent.dataIfPresent != null && parent.childNamesKnownRemoved != null) {
			
		}
	}
	
	// ------------------------------------------------------------------------
	
	protected PartialNodeEntry resolveOrNull(NodeNamesPath path) {
		return resolveUpTo(path, path.pathElements.length);
	}
	
	protected PartialNodeEntry resolveUpTo(NodeNamesPath path, int pathEltCount) {
		val pathElts = path.pathElements;
		PartialNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			val foundChildEntry = currEntry.getChild(pathElt);
			if (foundChildEntry == null) {
				return null; 
			} else {
				currEntry = foundChildEntry;
			}
		}
		return currEntry;
	}

	protected PartialNodeEntry resolveMkdirUpToParent(NodeNamesPath ofChildPath) {
		if (ofChildPath.pathElementCount() <= 1) {
			return rootNode;
		}
		return resolveMkdirUpTo(ofChildPath, ofChildPath.pathElements.length - 1);
	}

	protected PartialNodeEntry resolveMkdirUpTo(NodeNamesPath path, int pathEltCount) {
		val pathElts = path.pathElements;
		PartialNodeEntry currEntry = rootNode;
		for(int i = 0; i < pathEltCount; i++) {
			val pathElt = pathElts[i];
			if (currEntry.childEntries == null) {
				currEntry.childEntries = new TreeMap<>();
			}
			val foundChildEntry = currEntry.getChild(pathElt);
			if (foundChildEntry == null) {
				val childEntry = new PartialNodeEntry(pathElt, null, false, null, null);
				currEntry.childEntries.put(pathElt, childEntry);
				currEntry = childEntry;
			} else {
				currEntry = foundChildEntry;
			}
		}
		return currEntry;
	}

}
