package fr.an.attrtreestore.api;

public interface IPrefetchOtherReadTreeData {

	public NodeData get(NodeNamesPath path, PrefetchOtherNodeDataCallback optCallback);
	
}
