package fr.an.attrtreestore.api.readprefetch;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

/**
 * callbacks for handling dir listing information, 
 * and converting to NodeFsData with maybe incomplete info
 * 
 * as most FileSystem allow to fill directory child items info while listing parent directory
 * 
 * example on Azure DatalakeDirectoryClient.listPathes() => return List<PathItem> 
 *  => contains child name + type + len + lastModificationTime... but NOT creation time, neither ACLs..
 *
 */
public abstract class PrefetchNodeDataContext {
	
	public boolean acceptPrefectNodeDatas() { return true; }
	
	public abstract void onPrefetchNodeData(NodeNamesPath path, NodeData data, boolean isIncomplete);

	public boolean acceptRecurseProposePrefectPathItems() { return true; }
	
	public abstract PrefetchNodeDataContext createChildPrefetchContext();
	
	public abstract void onProposePrefetchPathItem(
			PrefetchProposedPathItem prefetchDataItem
			);

}
