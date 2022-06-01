package fr.an.attrtreestore.storage.api;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.Getter;
import lombok.val;

/**
 * union-fs like for Tree NodeData:
 * delegate update/deletes to override layer, else read-only queries to baseReadOnly layer
 *
 */
public class UnionOverrideRO_TreeNodeData {

	@Getter
	protected final ROCached_TreeNodeData baseReadOnlyTree;

	@Getter
	protected final PartialOverrideTreeNodeData overrideTree;
	
	// ------------------------------------------------------------------------
	
	public UnionOverrideRO_TreeNodeData(
			ROCached_TreeNodeData baseReadOnlyTree,
			PartialOverrideTreeNodeData overrideTree) {
		this.baseReadOnlyTree = baseReadOnlyTree;
		this.overrideTree = overrideTree;
	}

	// ------------------------------------------------------------------------
	
	// @Deprecated.. shoud use async Api?
	public NodeData get(NodeNamesPath path) {
		NodeOverrideData overrideData = overrideTree.get(path);
		if (overrideData != null) {
			switch(overrideData.status) {
			case DELETED: return null;
			case UPDATED: return overrideData.data;
			case NOT_OVERRIDEN: break; // cf next
			}
		}
		val res = baseReadOnlyTree.get(path);
		return res;
	}
	
	// public abstract CompletableFuture<NodeData> asyncGet(NodeNamesPath path);

	public void put(NodeNamesPath path, NodeData data) {
		overrideTree.put(path, data);
	}
	
	public void remove(NodeNamesPath path) {
		overrideTree.remove(path);
	}
	
}
