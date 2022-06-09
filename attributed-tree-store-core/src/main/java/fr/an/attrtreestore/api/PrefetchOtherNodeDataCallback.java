package fr.an.attrtreestore.api;

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
public abstract class PrefetchOtherNodeDataCallback {

	public abstract void onPrefetchOtherNodeData(NodeNamesPath path, NodeData data, boolean isIncomplete);
	
}
