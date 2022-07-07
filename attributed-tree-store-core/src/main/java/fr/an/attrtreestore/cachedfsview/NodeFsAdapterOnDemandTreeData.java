package fr.an.attrtreestore.cachedfsview;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.readprefetch.IPrefetchReadTreeDataSupport;
import fr.an.attrtreestore.api.readprefetch.PrefetchNodeDataContext;
import fr.an.attrtreestore.cachedfsview.converter.ConverterPrefetchNodeFsDataContext;
import fr.an.attrtreestore.cachedfsview.converter.NodeFsDataToNodeDataConverter;
import fr.an.attrtreestore.cachedfsview.converter.NodeFsDataToNodeDataConverter.DefaultNodeFsDataToNodeDataConverter;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.val;

public class NodeFsAdapterOnDemandTreeData extends TreeData implements IReadTreeData, IPrefetchReadTreeDataSupport {

	protected final NodeFsDataProvider fsDataProvider;

	protected final NodeFsDataToNodeDataConverter converter;
	
	// ------------------------------------------------------------------------

	public NodeFsAdapterOnDemandTreeData(NodeFsDataProvider fsDataProvider, NodeFsDataToNodeDataConverter converter) {
		this.fsDataProvider = fsDataProvider;
		this.converter = converter;
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
	public NodeData get(NodeNamesPath path, PrefetchNodeDataContext prefetchCtx) {
		val converterPrefetchCtx = (prefetchCtx != null)? new ConverterPrefetchNodeFsDataContext(prefetchCtx, converter): null;
		
		long refreshTimeMillis = System.currentTimeMillis(); // time client-side, before querying (otherwise may miss updates)

		NodeFsData fsData = fsDataProvider.queryNodeFsData(path, converterPrefetchCtx);

		if (fsData == null) {
			return null;
		}
		val nodeData = converter.nodeFsDataToNodeData(path, fsData, refreshTimeMillis, false);
		return nodeData;
	}

}
