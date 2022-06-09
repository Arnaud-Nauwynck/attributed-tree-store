package fr.an.attrtreestore.api;

import fr.an.attrtreestore.api.override.OverrideNodeData;

public interface IInMemCacheReadTreeData {

	// return OverrideNodeData instead of NodeData
	// may use another enum class with NOT_IN_CACHE,DELETED,UPDATED ... instead of NOT_OVERRIDEN,DELETED,UPDATED ? 
	// do noth use NodeData directly, otherwise caller could not know if null for deleted/not-exists or if not present in memory cache
	public OverrideNodeData getIfInMemCache(NodeNamesPath path);
	
}
