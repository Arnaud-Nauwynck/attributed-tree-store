package fr.an.attrtreestore.api;

import java.util.List;
import java.util.Map;

public interface IReadTreeData {

	public NodeData get(NodeNamesPath path);

//	// from IPrefetchOtherReadTreeData
//	public default NodeData get(NodeNamesPath path, PrefetchOtherNodeDataCallback optCallback) {
//		return get(path);
//	}

	public NodeData getWithChild(NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs);

}
