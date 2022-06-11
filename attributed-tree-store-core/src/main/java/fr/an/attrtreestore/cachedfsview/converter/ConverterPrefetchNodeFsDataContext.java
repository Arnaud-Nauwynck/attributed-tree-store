package fr.an.attrtreestore.cachedfsview.converter;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.readprefetch.PrefetchNodeDataContext;
import fr.an.attrtreestore.api.readprefetch.PrefetchProposedPathItem;
import fr.an.attrtreestore.cachedfsview.PrefetchNodeFsDataContext;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.val;

public class ConverterPrefetchNodeFsDataContext extends PrefetchNodeFsDataContext {

	private PrefetchNodeDataContext delegate;
	private NodeFsDataToNodeDataConverter converter;
	
	// ------------------------------------------------------------------------
	
	public ConverterPrefetchNodeFsDataContext(PrefetchNodeDataContext delegate,
			NodeFsDataToNodeDataConverter converter) {
		this.delegate = delegate;
		this.converter = converter;
	}

	// ------------------------------------------------------------------------
	
	@Override
	public boolean acceptPrefetchNodeDatas() { 
		return delegate.acceptPrefectNodeDatas(); 
	}
	
	@Override
	public void onPrefetchNodeFsData(NodeNamesPath path, NodeFsData fsDdata, long refreshTimeMillis, boolean isIncomplete) {
		val nodeData = converter.nodeFsDataToNodeData(path, fsDdata, 
				refreshTimeMillis, isIncomplete);
		delegate.onPrefetchNodeData(path, nodeData, isIncomplete);
	}

	@Override
	public boolean acceptRecurseProposePrefetchPathItems() { 
		return delegate.acceptRecurseProposePrefectPathItems();
	}
	
	@Override
	public PrefetchNodeFsDataContext createChildPrefetchFsContext() {
		PrefetchNodeDataContext childDelegate = delegate.createChildPrefetchContext();
		return new ConverterPrefetchNodeFsDataContext(childDelegate, converter);
	}

	@Override
	public void onProposePrefetchPathItem(
			PrefetchProposedPathItem prefetchDataItem
			) {
		delegate.onProposePrefetchPathItem(prefetchDataItem);
	}

}
