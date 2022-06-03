package fr.an.attrtreestore.cachedfsview;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.util.fsdata.NodeFsData;

public abstract class NodeFsDataProvider {

	/** query underlying FileSystem, using configured baseUrl + appending subpath */
	public abstract NodeFsData queryNodeFsData(NodeNamesPath subpath);
	
	// TOADD public abstract CompletableFuture<NodeFsData> asyncQueryNodeFsData(NodeNamesPath subpath);
	
}
