package fr.an.attrtreestore.api.readprefetch;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

public interface IPrefetchReadTreeDataSupport {

	public NodeData get(NodeNamesPath path, PrefetchNodeDataContext prefetchContext);

}
