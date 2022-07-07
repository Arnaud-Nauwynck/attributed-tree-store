package fr.an.attrtreestore.api;

import org.path4j.NodeNamesPath;
import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.MsgPrefixLoggingCallback;

import fr.an.attrtreestore.api.readprefetch.IPrefetchReadTreeDataSupport;
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
	@Getter
	protected final TreeData underlyingTree;
	protected final IPrefetchReadTreeDataSupport underlyingTree_supportsPrefetchOther; 
	
	@Getter
	protected final TCacheStorageTreeData cachedTree;
	protected final IInMemCacheReadTreeData cachedTree_supportsInMemCached; // = null or (ICacheReadTreeData)cachedTree
	
	protected boolean startedBackgroupRefreshesSupport;

	// protected List<NodeNamesPath path> pendingBackgroupRefreshes;

	@Getter @Setter
	protected int defaultCacheExpirationMillis = 10 * 60 * 1000; // 10 minutes
	@Getter @Setter
	protected int defaultUseCacheIfResponseExceedMillis = 3000; // 3 seconds

//	@Getter
//	private LoggingCounter cacheGetCounter = new LoggingCounter("cache.get");
	@Getter
	private LoggingCounter cacheGetHitCounter = new LoggingCounter("cache.get-hit");
	@Getter
	private LoggingCounter cacheGetHitButExpiredCounter = new LoggingCounter("cache.get-hit-but-expired");
	@Getter
	private LoggingCounter cacheGetMissCounter = new LoggingCounter("cache.get-miss");

	@Getter
	private LoggingCounter underlyingGetCounter = new LoggingCounter("underlying.get");

	// failed calls are counted twice in underlyingGetCounter + underlyingGetFailedCounter
	@Getter
	private LoggingCounter underlyingGetFailedCounter = new LoggingCounter("underlying.get-failed");

	// ------------------------------------------------------------------------

	public CachedImage_TreeData(String displayName, String displayBaseUrl, //
			TreeData underlyingTree, 
			TCacheStorageTreeData cachedTree) {
		this.displayName = displayName;
		this.displayBaseUrl = displayBaseUrl;
		this.underlyingTree = underlyingTree;
		this.underlyingTree_supportsPrefetchOther = (underlyingTree instanceof IPrefetchReadTreeDataSupport)? (IPrefetchReadTreeDataSupport) underlyingTree : null; 
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
		cacheGetHitCounter.incr(millis, logCallback);
	}

	protected void incrCacheGetHitButExpired(int millis, MsgPrefixLoggingCallback logCallback) {
		cacheGetHitButExpiredCounter.incr(millis, logCallback);
	}

	protected void incrCacheGetMiss(int millis, MsgPrefixLoggingCallback logCallback) {
		cacheGetMissCounter.incr(millis, logCallback);
	}

	protected void incrUnderlyingTreeGet(int millis, MsgPrefixLoggingCallback logCallback) {
		underlyingGetCounter.incr(millis, logCallback);
	}

	protected void incrUnderlyingTreeGetFailed(int millis, MsgPrefixLoggingCallback logCallback) {
		underlyingGetCounter.incr(millis, logCallback); // also increment count
		underlyingGetFailedCounter.incr(millis, logCallback);
	}
	
	public void setLoggingCountersFreq(int freq) {
		// cacheGetCount.setLogFreq(freq);
		cacheGetHitCounter.setLogFreq(freq);
		cacheGetHitButExpiredCounter.setLogFreq(freq);
		cacheGetMissCounter.setLogFreq(freq);
		underlyingGetCounter.setLogFreq(freq);
		underlyingGetFailedCounter.setLogFreq(freq);
	}

	public void setLoggingCountersMaxDelayMillis(int millis) {
		// cacheGetCount.setLogMaxDelayMillis(millis);
		cacheGetHitCounter.setLogMaxDelayMillis(millis);
		cacheGetHitButExpiredCounter.setLogMaxDelayMillis(millis);
		cacheGetMissCounter.setLogMaxDelayMillis(millis);
		underlyingGetCounter.setLogMaxDelayMillis(millis);
		underlyingGetFailedCounter.setLogMaxDelayMillis(millis);
	}

	public int getCacheGetHitCount() {
	    return cacheGetHitCounter.getCount();
	}
    public int getCacheGetHitButExpiredCount() {
        return cacheGetHitButExpiredCounter.getCount();
    }
    public int getCacheGetMissCount() {
        return cacheGetMissCounter.getCount();
    }
    public int getUnderlyingGetCount() {
        return underlyingGetCounter.getCount();
    }
    public int getUnderlyingGetFailedCount() {
        return underlyingGetFailedCounter.getCount();
    }

}
