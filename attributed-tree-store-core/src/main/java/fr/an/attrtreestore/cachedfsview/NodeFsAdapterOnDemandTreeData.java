package fr.an.attrtreestore.cachedfsview;

import fr.an.attrtreestore.api.IPrefetchOtherReadTreeData;
import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.PrefetchOtherNodeDataCallback;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.cachedfsview.NodeFsDataToNodeDataConverter.DefaultNodeFsDataToNodeDataConverter;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.val;

public class NodeFsAdapterOnDemandTreeData extends TreeData implements IReadTreeData, IPrefetchOtherReadTreeData {

	protected final NodeFsDataProvider delegate;

	protected final NodeFsDataToNodeDataConverter nodeDataConverter;
	
	// ------------------------------------------------------------------------

	public NodeFsAdapterOnDemandTreeData(NodeFsDataProvider delegate, NodeFsDataToNodeDataConverter nodeDataConverter) {
		this.delegate = delegate;
		this.nodeDataConverter = nodeDataConverter;
	}

	public NodeFsAdapterOnDemandTreeData(NodeFsDataProvider delegate) {
		this(delegate, DefaultNodeFsDataToNodeDataConverter.INSTANCE);
	}

	// ------------------------------------------------------------------------

	// should not be used... cf get(NodeNamesPath path, PrefetchOtherNodeDataCallback optCallback)
	// as most FileSystem may allow to fill directory child items (with incomplete data) while listing parent directory
	@Override
	public final NodeData get(NodeNamesPath path) {
		return get(path, null);
	}

	@Override
	public NodeData get(NodeNamesPath path, PrefetchOtherNodeDataCallback optCallback) {
		val converterCallback = (optCallback != null)? new ConverterNodeFsDataCallback(optCallback, nodeDataConverter): null;
		
		long refreshTimeMillis = System.currentTimeMillis(); // time client-side, before querying (otherwise may miss updates)

		NodeFsData fsData = delegate.queryNodeFsData(path, converterCallback);

		if (fsData == null) {
			return null;
		}
		val nodeData = nodeDataConverter.nodeFsDataToNodeData(path, fsData, refreshTimeMillis, false);
		return nodeData;
	}

}
