package fr.an.attrtreestore.storage.overrideapi;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData;
import fr.an.attrtreestore.api.TreeData;
import lombok.Getter;
import lombok.val;

/**
 * union-fs like for Tree NodeData:
 * delegate update/deletes to override layer, else read-only queries to baseReadOnly layer
 *
 */
public class UnionOverrideRO_TreeData extends TreeData implements IReadTreeData, IWriteTreeData {

	@Getter
	protected final ROCached_TreeData baseReadOnlyTree;

	@Getter
	protected final PartialOverrideTreeData overrideTree;
	
	// ------------------------------------------------------------------------
	
	public UnionOverrideRO_TreeData(
			ROCached_TreeData baseReadOnlyTree,
			PartialOverrideTreeData overrideTree) {
		this.baseReadOnlyTree = baseReadOnlyTree;
		this.overrideTree = overrideTree;
	}

	// ------------------------------------------------------------------------
	
	// @Deprecated.. shoud use async Api?
	@Override
	public NodeData get(NodeNamesPath path) {
		NodeOverrideData overrideData = overrideTree.getOverride(path);
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

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		overrideTree.put(path, data);
	}
	
	@Override
	public void remove(NodeNamesPath path) {
		overrideTree.remove(path);
	}
	
}
