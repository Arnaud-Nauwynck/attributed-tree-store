package fr.an.attrtreestore.cachedfsview;

import java.util.concurrent.Future;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class CachedFsView_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends TreeData implements IReadTreeData {

	protected final String displayName;
	protected final String displayBaseUrl;
	
	protected final NodeFsDataProvider fsDataProvider;
	
	protected final TCacheStorageTreeData cachedTree;

	protected boolean startedBackgroupRefreshes;

	// protected List<NodeNamesPath path> pendingBackgroupRefreshes;

	protected int defaultCacheExpiryMinutes = 10; // 10 minutes
	protected int defaultUseCacheIfResponseExceedMillis = 3000; // 3 seconds
	
	// ------------------------------------------------------------------------

	@Override
	public NodeData get(NodeNamesPath path) {
		int cacheExpiryMinutes = defaultCacheExpiryMinutes;
		long useCacheIfResponseExceedTime = System.currentTimeMillis() + defaultUseCacheIfResponseExceedMillis;
		return getCacheWaitMax(path, cacheExpiryMinutes, useCacheIfResponseExceedTime);
	}

	public abstract NodeData getCacheWaitMax(NodeNamesPath path, 
			int cacheExpiryMinutes,
			long useCacheIfResponseExceedTimeMillis);

	
	// ------------------------------------------------------------------------
	
	public boolean isStartedBackgroupRefreshes() {
		return startedBackgroupRefreshes;
	}

	public void startBackgroupRefreshes() {
		if (!startedBackgroupRefreshes) {
			this.startedBackgroupRefreshes = true;
			try {
				doStartBackgroupRefreshes();
			} catch(RuntimeException ex) {
				log.error("Failed doStartBackgroupRefreshes", ex);
				this.startedBackgroupRefreshes = false;
			}
		}
	}

	public void stopBackgroupRefreshes() {
		if (startedBackgroupRefreshes) {
			this.startedBackgroupRefreshes = false;
			try {
				doStopBackgroupRefreshes();
			} catch(RuntimeException ex) {
				log.error("Failed doStopBackgroupRefreshes", ex);
				this.startedBackgroupRefreshes = true;
			}
		}
	}

	protected abstract void doStartBackgroupRefreshes();
	protected abstract void doStopBackgroupRefreshes();
	

	// CompetableFuture ?
	public abstract Future<NodeFsData> submitBackgroupRefresh(NodeNamesPath path);

	public abstract NodeFsData doRefresh(NodeNamesPath path);
	
	
	// ------------------------------------------------------------------------
	
	@Override
	protected final void put(NodeNamesPath path, NodeData data) {
		throw new UnsupportedOperationException("method from IWritableTreeData should not be used here");
	}

	@Override
	protected final void remove(NodeNamesPath path) {
		throw new UnsupportedOperationException("method from IWritableTreeData should not be used here");
	}

}
