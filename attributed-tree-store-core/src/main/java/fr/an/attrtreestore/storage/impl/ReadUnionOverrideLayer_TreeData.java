package fr.an.attrtreestore.storage.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.override.OverrideNodeData;
import fr.an.attrtreestore.api.override.OverrideNodeStatus;
import fr.an.attrtreestore.api.override.OverrideTreeData;
import lombok.Getter;
import lombok.val;

/**
 * union-fs like for Tree NodeData:
 * delegate update/deletes to override layer, else read-only queries to baseReadOnly layer
 *
 */
public class ReadUnionOverrideLayer_TreeData extends TreeData implements IReadTreeData, IWriteTreeData {

	@Getter
	protected final ROCached_TreeData baseReadOnlyTree;

	@Getter
	protected final OverrideTreeData overrideTree;
	
	// ------------------------------------------------------------------------

	public ReadUnionOverrideLayer_TreeData(
			ROCached_TreeData baseReadOnlyTree,
			OverrideTreeData overrideTree) {
		this.baseReadOnlyTree = baseReadOnlyTree;
		this.overrideTree = overrideTree;
	}

	// ------------------------------------------------------------------------
	
	// @Deprecated.. shoud use async Api?
	@Override
	public NodeData get(NodeNamesPath path) {
		OverrideNodeData overrideData = overrideTree.getOverride(path);
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

	@Deprecated
	public NodeData getWithChild(NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		NodeData res = null;
		val foundOverrideChildMap = new HashMap<NodeName,OverrideNodeData>();
		val notFoundOverrideChildLs = new ArrayList<NodeName>();
		OverrideNodeData overrideData = overrideTree.getOverrideWithChild(path, foundOverrideChildMap, notFoundOverrideChildLs);
		if (overrideData != null) {
			switch(overrideData.status) {
			case DELETED: return null;
			case UPDATED: res = overrideData.data; break;
			case NOT_OVERRIDEN: res = null; break;
			}
		}
		HashMap<NodeName,NodeData> baseFoundChildMap = null;
		val baseNotFoundChildLs = new ArrayList<NodeName>(); 
		if (res == null) {
			baseFoundChildMap = new HashMap<NodeName,NodeData>(); 
			res = baseReadOnlyTree.getWithChild(path, baseFoundChildMap, baseNotFoundChildLs);
			if (res == null) {
				return null;
			}
		}
		val childNames = res.childNames;
		if (childNames != null && ! childNames.isEmpty()) {
			for (val childName: childNames) {
				val childOverride = foundOverrideChildMap.get(childName);
				if (childOverride != null && childOverride.status != OverrideNodeStatus.NOT_OVERRIDEN) {
					if (childOverride.status == OverrideNodeStatus.DELETED) {
						// do nothing
					} else {
						foundChildMap.put(childName, childOverride.data);
					}
				} else {
					if (baseFoundChildMap == null) {
						baseFoundChildMap = new HashMap<NodeName,NodeData>(); 
						baseReadOnlyTree.getWithChild(path, baseFoundChildMap, baseNotFoundChildLs);
					}
					val baseChildData = baseFoundChildMap.get(childName);
					if (baseChildData != null) {
						foundChildMap.put(childName, baseChildData);
					} else {
						notFoundChildLs.add(childName);
					}
				}
			}
		}
		return res;
	}

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		overrideTree.put(path, data);
	}
	
	@Override
	public void remove(NodeNamesPath path) {
		overrideTree.remove(path);
	}
	
}
