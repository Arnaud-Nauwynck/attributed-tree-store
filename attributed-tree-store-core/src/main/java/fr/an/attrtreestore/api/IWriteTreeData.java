package fr.an.attrtreestore.api;

public interface IWriteTreeData extends IReadTreeData { // TOADD should not extends interface IReadTreeData... but when not available, simply throw ex

	// TODO ... need to provide function to fill intermediate unknown nodes..
	// public abstract void put_createIntermediate(NodeNamesPath path, NodeData data, Function<NodeNamesPath,NodeData> func);
	// otherwise
	// public abstract void put_strictNoCreateIntermediate(NodeNamesPath path, NodeData data);
	

	public void put(NodeNamesPath path, NodeData data);
	
	public void remove(NodeNamesPath path);

	
	public default void put_transientFieldsChanged(NodeNamesPath path, NodeData data) {
		put(path, data);
	}

//	public put_transientFieldsChanged(NodeNamesPath path, 
//			long lastExternalRefreshTimeMillis,
//			int treeDataRecomputationMask,
//			int lruCount,
//			int lruAmortizedCount,
//			long lastTreeDataQueryTimeMillis
//			) {
//		throw new UnsupportedOperationException();		
//	}

}
