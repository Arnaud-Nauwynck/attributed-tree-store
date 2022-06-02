package fr.an.attrtreestore.storage.overrideapi;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

/**
 * abstract class equivalent to persisted <code> Map<NodeNamesPath, NodeData> </code>
 *
 */
public abstract class PartialOverrideTreeData implements IWriteTreeData {

	@Override
	public abstract void put(NodeNamesPath path, NodeData data);
	
	@Override
	public abstract void remove(NodeNamesPath path);
	
	/** @return override {data,status} <code>NodeOverrideData</code>, unlike 'get' for TreeData which return only NodeData */  
	public abstract NodeOverrideData getOverride(NodeNamesPath path);

	
	/** should not be called here, see getOverride() instead */
	@Override
	public NodeData get(NodeNamesPath path) {
		throw new UnsupportedOperationException("should not be called");
	}

}
