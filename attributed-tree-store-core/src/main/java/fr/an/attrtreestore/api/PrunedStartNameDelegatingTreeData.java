package fr.an.attrtreestore.api;

import org.path4j.NodeName;
import org.path4j.NodeNamesPath;

import lombok.Getter;

public class PrunedStartNameDelegatingTreeData<TTree extends TreeData & IWriteTreeData> extends TreeData implements IWriteTreeData {

	@Getter
	private final NodeName pruneStartName;
	
	@Getter
	private final TTree delegateTree;
	
	// ------------------------------------------------------------------------
	
	public PrunedStartNameDelegatingTreeData(NodeName pruneStartName, TTree delegateTree) {
		this.pruneStartName = pruneStartName;
		this.delegateTree = delegateTree;
	}

	// ------------------------------------------------------------------------

	@Override
	public NodeData get(NodeNamesPath path) {
		if (! path.startsWith(pruneStartName)) {
			return null;
		}
		NodeNamesPath underlyingPath = path.pruneStartPath(1);
		return delegateTree.get(underlyingPath);
	}

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		if (! path.startsWith(pruneStartName)) {
			throw new IllegalStateException();
		}
		NodeNamesPath underlyingPath = path.pruneStartPath(1);
		delegateTree.put(underlyingPath, data);
	}

	@Override
	public void remove(NodeNamesPath path) {
		if (! path.startsWith(pruneStartName)) {
			throw new IllegalStateException();
		}
		NodeNamesPath underlyingPath = path.pruneStartPath(1);
		delegateTree.remove(underlyingPath);
	}

}
