package fr.an.attrtreestore.api;

public interface IWriteTreeData {

	// TODO ... need to provide function to fill intermediate unknown nodes..
	// public abstract void put_createIntermediate(NodeNamesPath path, NodeData data, Function<NodeNamesPath,NodeData> func);
	// otherwise
	// public abstract void put_strictNoCreateIntermediate(NodeNamesPath path, NodeData data);
	

	public void put(NodeNamesPath path, NodeData data);
	
	public void remove(NodeNamesPath path);

}
