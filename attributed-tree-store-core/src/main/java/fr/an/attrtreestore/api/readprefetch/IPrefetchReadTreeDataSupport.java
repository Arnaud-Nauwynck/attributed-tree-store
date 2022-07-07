package fr.an.attrtreestore.api.readprefetch;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.NodeData;

public interface IPrefetchReadTreeDataSupport {

	public NodeData get(NodeNamesPath path, PrefetchNodeDataContext prefetchContext);

}
