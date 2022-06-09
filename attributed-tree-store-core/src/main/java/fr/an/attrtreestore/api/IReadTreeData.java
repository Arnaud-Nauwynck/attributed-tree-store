package fr.an.attrtreestore.api;

public interface IReadTreeData {

	public NodeData get(NodeNamesPath path);

//	// from IPrefetchOtherReadTreeData
//	public default NodeData get(NodeNamesPath path, PrefetchOtherNodeDataCallback optCallback) {
//		return get(path);
//	}

}
