package fr.an.attrtreestore.api;

import java.util.concurrent.Future;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class RefreshableCached_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends TreeData implements IReadTreeData {

	protected final String displayName;
	protected final String displayBaseUrl;
	
	protected final TreeData underlyingTree;
	
	protected final TCacheStorageTreeData cachedTree;

	protected boolean startedBackgroupRefreshesSupport;

	// protected List<NodeNamesPath path> pendingBackgroupRefreshes;

	protected int defaultCacheExpirationMillis = 10 * 60 * 1000; // 10 minutes
	protected int defaultUseCacheIfResponseExceedMillis = 3000; // 3 seconds
	
	protected long thresholdForBackgroundRefreshMillis = defaultCacheExpirationMillis - 1000; 
	
	private int cacheGetCount;
	private int cacheGetHitCount;
	private int cacheGetHitTotalMillis;
	private int cacheGetHitButExpiredCount;
	private int cacheGetHitButExpiredTotalMillis;
	private int cacheGetMissCount;
	private int cacheGetMissTotalMillis;

	private int underlyingGetCount;
	private long underlyingGetTotalMillis;
	private int underlyingGetFailedCount;
	private long underlyingGetFailedTotalMillis;
	
	// ------------------------------------------------------------------------

	@Override
	public NodeData get(NodeNamesPath path) {
		int cacheExpirationMillis = defaultCacheExpirationMillis;
		long useCacheIfResponseExceedTime = System.currentTimeMillis() + defaultUseCacheIfResponseExceedMillis;
		return getCacheWaitMax(path, cacheExpirationMillis, useCacheIfResponseExceedTime);
	}

	public abstract NodeData getCacheWaitMax(NodeNamesPath path, 
			int cacheExpirationMillis,
			long useCacheIfResponseExceedTimeMillis);


	// ------------------------------------------------------------------------
	
	protected void incrCacheGetHit(int millis) {
		cacheGetCount++;
		cacheGetHitCount++;
		cacheGetHitTotalMillis += millis;
	}

	protected void incrCacheGetHitButExpired(int millis) {
		cacheGetCount++;
		cacheGetHitButExpiredCount++;
		cacheGetHitButExpiredTotalMillis += millis;
	}

	protected void incrCacheGetMiss(int millis) {
		cacheGetCount++;
		cacheGetMissCount++;
		cacheGetMissTotalMillis += millis;
	}

	protected void incrUnderlyingTreeGet(int millis) {
		underlyingGetCount++;
		underlyingGetTotalMillis += millis;
	}

	protected void incrUnderlyingTreeGetFailed(int millis, Exception ex) {
		underlyingGetCount++; // also increment count
		underlyingGetFailedCount++;
		underlyingGetFailedTotalMillis += millis;
	}
	
	// ------------------------------------------------------------------------
	
	public boolean isStartedBackgroupRefreshSupport() {
		return startedBackgroupRefreshesSupport;
	}

	public void startBackgroupRefreshSupport() {
		if (!startedBackgroupRefreshesSupport) {
			this.startedBackgroupRefreshesSupport = true;
			try {
				doStartBackgroupRefreshSupport();
			} catch(RuntimeException ex) {
				log.error("Failed doStartBackgroupRefreshes", ex);
				this.startedBackgroupRefreshesSupport = false;
			}
		}
	}

	public void stopBackgroupRefreshSupport() {
		if (startedBackgroupRefreshesSupport) {
			this.startedBackgroupRefreshesSupport = false;
			try {
				doStopBackgroupRefreshSupport();
			} catch(RuntimeException ex) {
				log.error("Failed doStopBackgroupRefreshes", ex);
				this.startedBackgroupRefreshesSupport = true;
			}
		}
	}

	protected abstract void doStartBackgroupRefreshSupport();
	protected abstract void doStopBackgroupRefreshSupport();
	

	// CompetableFuture ?
	public abstract Future<Void> submitBackgroupRefreshIfStarted(NodeNamesPath path);

	public abstract void doRefresh(NodeNamesPath path);
	

}
