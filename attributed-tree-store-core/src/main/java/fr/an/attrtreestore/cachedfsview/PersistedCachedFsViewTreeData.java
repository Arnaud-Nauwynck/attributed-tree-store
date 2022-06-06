package fr.an.attrtreestore.cachedfsview;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.impl.DefaultCached_TreeData;
import fr.an.attrtreestore.storage.impl.PersistedTreeData;
import lombok.Getter;

public class PersistedCachedFsViewTreeData // extends RefreshableCached_TreeData<PersistedTreeData> 
	{

	@Getter
	private final DefaultCached_TreeData<PersistedTreeData> tree;

	private final PersistedTreeData cachePersistedTree;
	
	// private final NodeFsAdapterTreeData underlyingFsAdapterTree;

	// ------------------------------------------------------------------------
	
	public PersistedCachedFsViewTreeData(
			String displayName,
			String displayBaseUrl,
			NodeFsAdapterTreeData underlyingFsAdapterTree, 
			BlobStorage blobStorage, String baseDirname,
			AttrDataEncoderHelper attrDataEncoderHelper
			) {
		this.cachePersistedTree = new PersistedTreeData(blobStorage, baseDirname, attrDataEncoderHelper);
		
		this.tree = new DefaultCached_TreeData<PersistedTreeData>(
				displayName, displayBaseUrl,
				underlyingFsAdapterTree,
				cachePersistedTree);
	}
	
	// ------------------------------------------------------------------------

	public NodeData getCacheWaitMax(NodeNamesPath path, 
			int cacheExpirationMillis,
			long useCacheIfResponseExceedTimeMillis) {
		return tree.getCacheWaitMax(path, cacheExpirationMillis, useCacheIfResponseExceedTimeMillis);
	}

////	@Override
//	protected void doStartBackgroupRefreshSupport() {
//		tree.doStartBackgroupRefreshSupport();
//	}
//
////	@Override
//	protected void doStopBackgroupRefreshSupport() {
//		tree.doStopBackgroupRefreshSupport();
//	}
//
////	@Override
//	public Future<Void> submitBackgroupRefreshIfStarted(NodeNamesPath path) {
//		return tree.submitBackgroupRefreshIfStarted(path);
//	}
//
////	@Override
//	public void doRefresh(NodeNamesPath path) {
//		tree.doRefresh(path);
//	}

}
