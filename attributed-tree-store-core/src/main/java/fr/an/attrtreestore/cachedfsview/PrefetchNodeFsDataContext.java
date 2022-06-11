package fr.an.attrtreestore.cachedfsview;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.readprefetch.PrefetchProposedPathItem;
import fr.an.attrtreestore.util.fsdata.NodeFsData;

/**
 * same as PrefetchNodeDataContext, but using NodeFsData insteead of NodeData
 * .. cf ConverterPrefetchNodeFsDataContext
 */
public abstract class PrefetchNodeFsDataContext {

	public abstract boolean acceptPrefetchNodeDatas();
	
	public abstract void onPrefetchNodeFsData(
			NodeNamesPath path, NodeFsData fsDdata, long lastRefreshTime, boolean isIncomplete);

	public abstract boolean acceptRecurseProposePrefetchPathItems();
	
	public abstract PrefetchNodeFsDataContext createChildPrefetchFsContext();
	
	public abstract void onProposePrefetchPathItem(
			PrefetchProposedPathItem prefetchDataItem);
	
}
