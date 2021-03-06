package fr.an.attrtreestore.cachedfsview;

import org.path4j.NodeNamesPath;
import org.simplestorage4j.api.BlobStorage;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.impl.DefaultSyncCachedImage_TreeData;
import fr.an.attrtreestore.storage.impl.PersistedTreeData;
import lombok.Getter;

/**
 * @Deprecated? helper class for composing
 * <code>
 * new DefaultCached_TreeData<PersistedTreeData>(displayName, displayBaseUrl,
 *				underlyingFsAdapterTree,
 *				new PersistedTreeData(blobStorage, baseDirname, attrDataEncoderHelper));
 * </code>
 *
 */
public class PersistedCachedFsViewTreeData // extends RefreshableCached_TreeData<PersistedTreeData> 
	{

	@Getter
	private final DefaultSyncCachedImage_TreeData<PersistedTreeData> tree;

	private final PersistedTreeData cachePersistedTree;
	
	// private final NodeFsAdapterTreeData underlyingFsAdapterTree;

	// ------------------------------------------------------------------------
	
	public PersistedCachedFsViewTreeData(
			String displayName,
			String displayBaseUrl,
			NodeFsAdapterOnDemandTreeData underlyingFsAdapterTree, 
			BlobStorage blobStorage, String baseDirname,
			AttrDataEncoderHelper attrDataEncoderHelper
			) {
		this.cachePersistedTree = new PersistedTreeData(blobStorage, baseDirname, attrDataEncoderHelper);
		
		this.tree = new DefaultSyncCachedImage_TreeData<PersistedTreeData>(
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
