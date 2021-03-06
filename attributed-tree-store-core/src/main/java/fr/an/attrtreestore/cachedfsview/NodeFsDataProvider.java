package fr.an.attrtreestore.cachedfsview;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.util.fsdata.NodeFsData;

public abstract class NodeFsDataProvider {

//	public NodeFsData queryNodeFsData(NodeNamesPath subpath) {
//		return queryNodeFsData(subpath, null);
//	}

	// TOADD public abstract CompletableFuture<NodeFsData> asyncQueryNodeFsData(NodeNamesPath subpath, PrefetchNodeFsDataContext prefetchCtx);
	
	/** query underlying FileSystem, using configured baseUrl + appending subpath 
	 * optionnally use <code>optCallback</code> to also build directo 
	 */
	public abstract NodeFsData queryNodeFsData(NodeNamesPath path, PrefetchNodeFsDataContext prefetchCtx);

}
