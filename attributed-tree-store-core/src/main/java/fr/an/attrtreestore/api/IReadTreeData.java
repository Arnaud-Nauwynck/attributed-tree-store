package fr.an.attrtreestore.api;

import java.util.List;
import java.util.Map;

import fr.an.attrtreestore.util.TreeDataUtils;

public interface IReadTreeData {

	public NodeData get(NodeNamesPath path);

	
	// may override for optims
	public default NodeData getWithChild(NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		return TreeDataUtils.getWithChild(this, path, foundChildMap, notFoundChildLs);
	}

}
