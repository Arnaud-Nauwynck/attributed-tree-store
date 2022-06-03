package fr.an.attrtreestore.api.override;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

/**
 * abstract class equivalent to persisted <code> Map<NodeNamesPath, NodeOverrideData> </code>
 *
 * cf sub-classes implementation:
 * - AppendBlobStorage_PartialOverrideTreeNodeData
 */
public abstract class OverrideTreeData implements IWriteTreeData {

	@Override
	public abstract void put(NodeNamesPath path, NodeData data);
	
	@Override
	public abstract void remove(NodeNamesPath path);
	
	/** @return override {data,status} <code>NodeOverrideData</code>, unlike 'get' for TreeData which return only NodeData */  
	public abstract OverrideNodeData getOverride(NodeNamesPath path);

	
	/** should not be called here, see getOverride() instead 
	 * internally: because interface IWriteTreeData extends IReadTreeData 
	 */
	@Override
	public NodeData get(NodeNamesPath path) {
		throw new UnsupportedOperationException("should not be called");
	}

}
