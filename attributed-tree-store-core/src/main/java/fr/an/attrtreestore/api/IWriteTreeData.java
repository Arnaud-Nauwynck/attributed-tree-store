package fr.an.attrtreestore.api;

import java.util.Collection;

import lombok.val;

public interface IWriteTreeData extends IReadTreeData { // TOADD should not extends interface IReadTreeData... but when not available, simply throw ex

	// TODO ... need to provide function to fill intermediate unknown nodes..
	// public abstract void put_createIntermediate(NodeNamesPath path, NodeData data, Function<NodeNamesPath,NodeData> func);
	// otherwise
	// public abstract void put_strictNoCreateIntermediate(NodeNamesPath path, NodeData data);
	

	public void put(NodeNamesPath path, NodeData data);
	
	public void remove(NodeNamesPath path);

	// may override for optims
	default public void putWithChild(NodeNamesPath path, NodeData data, Collection<NodeData> childDatas) {
	    put(path, data);
	    putChildList(path, childDatas);
	}
	
	default public void putChildList(NodeNamesPath parentPath, Collection<NodeData> childDatas) {
    	for(val childData: childDatas) {
            val childPath = parentPath.toChild(childData.name);
            put(childPath, childData);
        }
	}

	
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
