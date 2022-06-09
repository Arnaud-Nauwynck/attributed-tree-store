package fr.an.attrtreestore.storage.impl;

import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import lombok.val;

public class Guava_LoadingCacheTreeData extends TreeData {

	private final LoadingCache<NodeNamesPath, NodeData> cache;
	
	// ------------------------------------------------------------------------
	
	public Guava_LoadingCacheTreeData(IReadTreeData delegateLoader, int maxCacheSize) {
		val guavaCacheLoader = new CacheLoader<NodeNamesPath, NodeData>() {
	        @Override
	        public NodeData load(NodeNamesPath key) {
	            return delegateLoader.get(key);
	        }
	    };
	    this.cache = CacheBuilder.newBuilder()
	    		.softValues()
	    		.maximumSize(maxCacheSize)
	    		.build(guavaCacheLoader);
	}
	
	// implements IReadTreeData
	// ------------------------------------------------------------------------
	
	@Override
	public NodeData get(NodeNamesPath path) {
		NodeData res;
		try {
			res = cache.get(path);
		} catch (ExecutionException ex) {
			throw new RuntimeException(ex);
		}
		return res;
	}
	
}
