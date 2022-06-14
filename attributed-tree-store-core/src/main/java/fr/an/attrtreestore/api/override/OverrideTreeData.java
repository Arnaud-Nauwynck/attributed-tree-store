package fr.an.attrtreestore.api.override;

import java.util.List;
import java.util.Map;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.val;

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

	// should override for optims
	public OverrideNodeData getOverrideWithChild(NodeNamesPath path,
			Map<NodeName,OverrideNodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		return defaultGetOverrideWithChild(path, foundChildMap, notFoundChildLs);
	}

	protected OverrideNodeData defaultGetOverrideWithChild(NodeNamesPath path,
			Map<NodeName,OverrideNodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		OverrideNodeData res = getOverride(path);
		if (res == null) {
			return OverrideNodeData.NOT_OVERRIDEN;
		} else if (res.status == OverrideNodeStatus.DELETED) {
			return res;
		} else {
			val data = res.data;
			if (data == null) return res; // should not occur
			val childNames = data.childNames;
			if (childNames != null && ! childNames.isEmpty()) {
				for(val childName: childNames) {
					val childPath = path.toChild(childName); 
					val childOverrideData = getOverride(childPath);
					if (childOverrideData != null) {
						foundChildMap.put(childName, childOverrideData);
					} else {
						notFoundChildLs.add(childName);
					}
				}
			}		
			return res;
		}
	}
	
	/** should not be called here, see getOverride() instead 
	 * internally: because interface IWriteTreeData extends IReadTreeData 
	 */
	@Override
	public NodeData get(NodeNamesPath path) {
		throw new UnsupportedOperationException("should not be called");
	}

	/** should not be called here, see getOverride() instead 
	 * internally: because interface IWriteTreeData extends IReadTreeData 
	 */
	@Override
	public NodeData getWithChild(NodeNamesPath path, 
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		throw new UnsupportedOperationException("should not be called");
	}

	
}
