package fr.an.attrtreestore.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.Map;

import lombok.val;

/**
 * 
 */
public class TopLevelMountTreeData<TTree extends TreeData & IWriteTreeData> extends TreeData implements IWriteTreeData {

	private Map<NodeName, PrunedStartNameDelegatingTreeData<TTree>> mountTrees;
	
	private NodeData rootData;
	
	// ------------------------------------------------------------------------
	
	public TopLevelMountTreeData(Map<NodeName, TTree> mounts) {
		val mountTrees = new LinkedHashMap<NodeName, PrunedStartNameDelegatingTreeData<TTree>>(); 
		for(val e: mounts.entrySet()) {
			val name = e.getKey();
			val tree = e.getValue();
			val delegateSubTree = new PrunedStartNameDelegatingTreeData<TTree>(name, tree);
			mountTrees.put(name, delegateSubTree);
		}
		this.mountTrees = mountTrees;
		updateRootData();
	}

	// ------------------------------------------------------------------------

	public void addMount(NodeName name, TTree subTree) {
		val mountTrees = new LinkedHashMap<NodeName, PrunedStartNameDelegatingTreeData<TTree>>(this.mountTrees);
		val delegateSubTree = new PrunedStartNameDelegatingTreeData<TTree>(name, subTree);
		mountTrees.put(name, delegateSubTree);
		this.mountTrees = mountTrees;
		updateRootData();
	}

	public void removeMount(NodeName name) {
		val mountTrees = new LinkedHashMap<NodeName, PrunedStartNameDelegatingTreeData<TTree>>(this.mountTrees);
		mountTrees.remove(name);
		this.mountTrees = mountTrees;
		updateRootData();
	}

	protected void updateRootData() {
		val name = NodeName.EMPTY;
		val childNames = ImmutableSet.<NodeName>copyOf(this.mountTrees.keySet());
		val attrs = ImmutableMap.<String,NodeAttr>of();
		val now = System.currentTimeMillis();
		val externalCreationTime = 0L;
		val externalLastModifiedTime = now;
		val externalLength = 0L;
		val lastExternalRefreshTimeMillis = 0L;
		val lastTreeDataUpdateTimeMillis = now;
		val lastTreeDataUpdateCount = 0;
		val treeDataRecomputationMask = 0;
		val lruCount = 0;
		val lruAmortizedCount = 0;
		val lastTreeDataQueryTimeMillis = 0L;
		
		this.rootData = new NodeData(name, NodeData.TYPE_DIR, 0, childNames, attrs, //
				externalCreationTime, externalLastModifiedTime, externalLength, //
				lastExternalRefreshTimeMillis, lastTreeDataUpdateTimeMillis, //
				lastTreeDataUpdateCount, treeDataRecomputationMask, //
				lruCount, lruAmortizedCount, lastTreeDataQueryTimeMillis
				);
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public NodeData get(NodeNamesPath path) {
		if (path.pathElements.length == 0) {
			return rootData;
		}
		NodeName firstName = path.pathElements[0]; 
		val subTree = mountTrees.get(firstName);
		if (subTree == null) {
			return null;
		}
		NodeNamesPath underlyingPath = path.pruneStartPath(1);
		return subTree.get(underlyingPath);
	}

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		if (path.pathElements.length == 0) {
			throw new UnsupportedOperationException();
		}
		NodeName firstName = path.pathElements[0]; 
		val subTree = mountTrees.get(firstName);
		if (subTree == null) {
			throw new IllegalArgumentException();
		}
		NodeNamesPath underlyingPath = path.pruneStartPath(1);
		subTree.put(underlyingPath, data);
	}

	@Override
	public void remove(NodeNamesPath path) {
		if (path.pathElements.length == 0) {
			throw new UnsupportedOperationException();
		}
		NodeName firstName = path.pathElements[0]; 
		val subTree = mountTrees.get(firstName);
		if (subTree == null) {
			return; // do nothing?
		}
		NodeNamesPath underlyingPath = path.pruneStartPath(1);
		subTree.remove(underlyingPath);
	}

}
