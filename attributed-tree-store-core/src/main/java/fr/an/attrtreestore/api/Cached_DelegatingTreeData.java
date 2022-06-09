package fr.an.attrtreestore.api;

import lombok.Getter;
import lombok.Setter;

/**
 * delegating TreeData get(path) to underlyingTree, and caching results.
 */
public abstract class Cached_DelegatingTreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
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
	private int cacheGetCount;
	@Getter
	private int cacheGetHitCount;
	@Getter
	private int cacheGetHitTotalMillis;
	@Getter
	private int cacheGetHitButExpiredCount;
	@Getter
	private int cacheGetHitButExpiredTotalMillis;
	@Getter
	private int cacheGetMissCount;
	@Getter
	private int cacheGetMissTotalMillis;

	@Getter
	private int underlyingGetCount;
	@Getter
	private long underlyingGetTotalMillis;
	@Getter
	private int underlyingGetFailedCount;
	@Getter
	private long underlyingGetFailedTotalMillis;

	// ------------------------------------------------------------------------

	public Cached_DelegatingTreeData(String displayName, String displayBaseUrl, //
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
	
	
}
