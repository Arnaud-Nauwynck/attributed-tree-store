package fr.an.attrtreestore.api;

import fr.an.attrtreestore.util.LoggingCounter;
import fr.an.attrtreestore.util.LoggingCounter.MsgPrefixLoggingCallback;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * delegating TreeData get(path) to underlyingTree, and caching results.
 */
@Slf4j
public abstract class CachedImage_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends TreeData implements IReadTreeData {

	protected final String displayName;
	protected final String displayBaseUrl;
	
	/** underlying tree (mostly converter from NodeFsData to NodeData?)
	 * may supports IPrefetchOtherReadTreeData
	 * => so preferrably use 'NodeData get(NodeNamesPath path, PrefetchOtherNodeDataCallback optCallback)'
	 * rather than 'NodeData get(NodeNamesPath path)'
	 */
	protected final TreeData underlyingTree;
	protected final IPrefetchOtherReadTreeData underlyingTree_supportsPrefetchOther; 
	
	protected final TCacheStorageTreeData cachedTree;
	protected final IInMemCacheReadTreeData cachedTree_supportsInMemCached; // = null or (ICacheReadTreeData)cachedTree
	
	protected boolean startedBackgroupRefreshesSupport;

	// protected List<NodeNamesPath path> pendingBackgroupRefreshes;

	@Getter @Setter
	protected int defaultCacheExpirationMillis = 10 * 60 * 1000; // 10 minutes
	@Getter @Setter
	protected int defaultUseCacheIfResponseExceedMillis = 3000; // 3 seconds

	@Getter
	private LoggingCounter cacheGetCount = new LoggingCounter("cache.get");
	@Getter
	private LoggingCounter cacheGetHitCount = new LoggingCounter("cache.get-hit");
	@Getter
	private LoggingCounter cacheGetHitButExpiredCount = new LoggingCounter("cache.get-hit-but-expired");
	@Getter
	private LoggingCounter cacheGetMissCount = new LoggingCounter("cache.get-miss");

	@Getter
	private LoggingCounter underlyingGetCount = new LoggingCounter("underlying.get");

	// failed calls are counted twice in underlyingGetCount + underlyingGetFailedCount 
	@Getter
	private LoggingCounter underlyingGetFailedCount = new LoggingCounter("underlying.get-failed");

	// ------------------------------------------------------------------------

	public CachedImage_TreeData(String displayName, String displayBaseUrl, //
			TreeData underlyingTree, 
			TCacheStorageTreeData cachedTree) {
		this.displayName = displayName;
		this.displayBaseUrl = displayBaseUrl;
		this.underlyingTree = underlyingTree;
		this.underlyingTree_supportsPrefetchOther = (underlyingTree instanceof IPrefetchOtherReadTreeData)? (IPrefetchOtherReadTreeData) underlyingTree : null; 
		this.cachedTree = cachedTree;
		this.cachedTree_supportsInMemCached = (cachedTree instanceof IInMemCacheReadTreeData)? (IInMemCacheReadTreeData) cachedTree : null; 
	}
	
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
	
	protected void incrCacheGetHit(int millis, MsgPrefixLoggingCallback logCallback) {
		cacheGetCount.incr(millis, logCallback);
	}

	protected void incrCacheGetHitButExpired(int millis, MsgPrefixLoggingCallback logCallback) {
		cacheGetCount.incr(millis, logCallback);
	}

	protected void incrCacheGetMiss(int millis, MsgPrefixLoggingCallback logCallback) {
		cacheGetCount.incr(millis, logCallback);
	}

	protected void incrUnderlyingTreeGet(int millis, MsgPrefixLoggingCallback logCallback) {
		underlyingGetCount.incr(millis, logCallback);
	}

	protected void incrUnderlyingTreeGetFailed(int millis, MsgPrefixLoggingCallback logCallback) {
		underlyingGetCount.incr(millis, logCallback); // also increment count
		underlyingGetFailedCount.incr(millis, logCallback);
	}
	
	public void setLoggingCountersFreq(int freq) {
		cacheGetCount.setLogFreq(freq);
		cacheGetHitCount.setLogFreq(freq);
		cacheGetHitButExpiredCount.setLogFreq(freq);
		cacheGetMissCount.setLogFreq(freq);
		underlyingGetCount.setLogFreq(freq);
		underlyingGetFailedCount.setLogFreq(freq);
	}

	public void setLoggingCountersMaxDelayMillis(int millis) {
		cacheGetCount.setLogMaxDelayMillis(millis);
		cacheGetHitCount.setLogMaxDelayMillis(millis);
		cacheGetHitButExpiredCount.setLogMaxDelayMillis(millis);
		cacheGetMissCount.setLogMaxDelayMillis(millis);
		underlyingGetCount.setLogMaxDelayMillis(millis);
		underlyingGetFailedCount.setLogMaxDelayMillis(millis);
	}

}
