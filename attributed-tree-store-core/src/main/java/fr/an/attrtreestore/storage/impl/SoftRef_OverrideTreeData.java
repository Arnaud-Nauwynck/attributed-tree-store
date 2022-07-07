package fr.an.attrtreestore.storage.impl;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.override.OverrideNodeData;
import fr.an.attrtreestore.api.override.OverrideTreeData;

/**
 * contains SoftReferences of OverrideNodeData
 * ... to be used as cache storage with DefaultRefreshableCached_DelegatingTreeData
 *  
 * ... at any time, any entry can disapear from cache (from in-memory SoftReference)
 * 
 * TODO NOT IMPLEMENTED YET
 * current implementation tries to defines SoftReference at deepest levels
 * so that GC should only remove terminal child, layer by layer as long as required, but not whole sub-tree
 *  
 */
public class SoftRef_OverrideTreeData extends OverrideTreeData {

	// SoftReference<T>;
	
	// ------------------------------------------------------------------------
	
	// TODO NOT IMPLEMENTED YET .. cf Guava_LoadingCacheTreeData
	private SoftRef_OverrideTreeData() {
	}
	
	// implements OverrideTreeData
	// ------------------------------------------------------------------------


	@Override
	public OverrideNodeData getOverride(NodeNamesPath path) {
		// TODO Auto-generated method stub
		return null;
	}

	
	// implements IWriteTreeData
	// ------------------------------------------------------------------------
	
	@Override
	public void put(NodeNamesPath path, NodeData data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(NodeNamesPath path) {
		// TODO Auto-generated method stub
		
	}
	
}
