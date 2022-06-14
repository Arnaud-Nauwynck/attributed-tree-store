package fr.an.attrtreestore.storage.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.SyncCachedImage_TreeData;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.override.OverrideNodeStatus;
import fr.an.attrtreestore.api.readprefetch.LimitingPrefetchNodeDataContext;
import fr.an.attrtreestore.api.readprefetch.LimitingPrefetchNodeDataContext.PrefetchCount;
import fr.an.attrtreestore.api.readprefetch.LimitingPrefetchNodeDataContext.PrefetchLimitParams;
import fr.an.attrtreestore.api.readprefetch.LimitingPrefetchNodeDataContext.PrefetchTimeLimit;
import fr.an.attrtreestore.api.readprefetch.PrefetchNodeDataContext;
import fr.an.attrtreestore.api.readprefetch.PrefetchProposedPathItem;
import fr.an.attrtreestore.util.DefaultNamedThreadFactory;
import fr.an.attrtreestore.util.LoggingCounter;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * delegating TreeData get(path) to underlyingTree, and caching results.
 * when discovering changes in parent->child, not present in cache, support for prefetching/resolving change
 */
@Slf4j
public class DefaultSyncCachedImage_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends SyncCachedImage_TreeData<TCacheStorageTreeData> {

	protected boolean ownedBackgroundRefreshExecutorService;
	private final ThreadFactory backgroundRefreshThreadFactory;
	private final BlockingQueue<Runnable> backgroundRefreshQueue = new SynchronousQueue<Runnable>();
	protected boolean isRunningBackgroundRefresh;
	// ExecutorService, may be shared, otherwise created on demand and marked as owned for later shutdown()
	protected ExecutorService backgroundRefreshExecutorService;

	protected boolean ownedBackgroundChangedResolverExecutorService;
	private boolean useCommonForkJoinPoolForBackgroundChangedResolver = true;
	private final ThreadFactory backgroundChangeResolverThreadFactory;
	// ExecutorService, may be shared, otherwise created on demand and marked as owned for later shutdown()
	protected ExecutorService backgroundChangeResolverExecutorService;
	
	protected enum PendingRefreshStatus {
		REFRESH_CACHE_EXPIRED,
		RESOLVE_CHANGE_DETECTED,
		CANCEL_TASK_SKIP_REFRESH,
		IN_PROGRESS
	}
	// do not submit immediatly to ExecutorService... some task may be auto-cleared later, but before processing
	protected class PendingRefreshTask {
		private final NodeNamesPath path;
		PendingRefreshStatus status;
		Future<?> submitedFuture;
		
		public PendingRefreshTask(NodeNamesPath path, PendingRefreshStatus status) {
			this.path = path;
			this.status = status;
		}
	}
	
	private final Object pendingRefreshsLock = new Object();

	// @GuardedBy("pendingRefreshsLock")
	private Map<NodeNamesPath,PendingRefreshTask> pendingCacheExpiredTasks = new LinkedHashMap<>();
	private int pendingCacheExpiredTaskProcessingCount; // count +1 and -1 at each start/end processing
	private int pendingCacheExpiredTaskSubmittedCount; // count +1 and -1 at each submit / dequeued
	@Getter @Setter
	private int maxSubmittingPendingRefreshTaskCount = 10;
	
	// @GuardedBy("pendingRefreshsLock")
	private Map<NodeNamesPath,PendingRefreshTask> pendingResolveChangeTasks = new LinkedHashMap<>();
	private int pendingResolveChangeTaskProcessingCount; // count +1 and -1 at each start/end processing
	private int pendingResolveChangeTaskSubmittedCount; // count +1 and -1 at each submit / dequeued
	@Getter @Setter
	private int maxSubmittingResolveChangeTaskCount = 50;

	private PrefetchLimitParams prefetchLimitParams = new PrefetchLimitParams(
			100_000, // maxCount;
			3, // maxRecurseLevel
			5000 // maxDurationMillis
			);
	
	private final PrefetchNodeDataContext prefetchNodeDataCallback = new PrefetchNodeDataContext() {
		@Override
		public void onPrefetchNodeData(NodeNamesPath path, NodeData data, boolean isIncomplete) {
			safeOnPrefetchOtherNodeData(path, data, isIncomplete);
		}
		@Override
		public void onProposePrefetchPathItem(PrefetchProposedPathItem prefetchDataItem) {
			safeOnProposePrefetchPathItem(prefetchDataItem);
		}
		@Override
		public PrefetchNodeDataContext createChildPrefetchContext() {
			return this;
		}
	};

    protected final LoggingCounter underlyingTree_get_resyncDeletedAncestorCounter = new LoggingCounter("underlying.get-resync-deleted-ancestor");
    protected final LoggingCounter underlying_prefetchedOtherNode_Counter = new LoggingCounter("underlying-prefetchedOtherNode");

    enum CachePutReason {
        underlyingGet,
        underlyingGet_expired,
        underlyingPrefetchOther,
        resyncDeletedAncestor,
    }
    protected final LoggingCounter cachedTree_put_underlyingGet_Counter = new LoggingCounter("cache.put-underlyingGet");
    protected final LoggingCounter cachedTree_put_underlyingGet_expired_Counter = new LoggingCounter("cache.put-underlyingGet-expired");
    protected final LoggingCounter cachedTree_put_underlyingPrefetchOther_Counter = new LoggingCounter("cache.put-underlyingPrefetchOther");
    protected final LoggingCounter cachedTree_put_resyncDeletedAncestor_Counter = new LoggingCounter("cache.put-resyncDeletedAncestor");

    protected final LoggingCounter cachedTree_putTransient_underlyingGet_Counter = new LoggingCounter("cache.putTransient-underlyingGet");
    protected final LoggingCounter cachedTree_putTransient_underlyingGet_expired_Counter = new LoggingCounter("cache.putTransient-underlyingGet-expired");
    protected final LoggingCounter cachedTree_putTransient_underlyingPrefetchOther_Counter = new LoggingCounter("cache.putTransient-underlyingPrefetchOther");
    protected final LoggingCounter cachedTree_putTransient_resyncDeletedAncestor_Counter = new LoggingCounter("cache.putTransient-resyncDeletedAncestor");

    protected final LoggingCounter cachedTree_remove_Counter = new LoggingCounter("cache.remove");
    // TOADD protected final LoggingCounter cachedTree_remove_resyncDeletedAncestor_Counter = new LoggingCounter("cache.remove-resyncDeletedAncestor");
    
//    protected final LoggingCounter cachedTree_put_transienFieldsChange_Counter = new LoggingCounter("cache.put-transientFieldsChange");
    protected final LoggingCounter cachedTree_get_resyncDeletedAncestorCounter = new LoggingCounter("cache.get-resync-deleted-ancestor");
    
	// ------------------------------------------------------------------------

	public DefaultSyncCachedImage_TreeData(
			String displayName,
			String displayBaseUrl,
			TreeData underlyingTree, // may implements IPrefetchOtherReadTreeData 
			TCacheStorageTreeData cachedTree // may implements IInMemCacheReadTreeData
			) {
		super(displayName, displayBaseUrl, underlyingTree, cachedTree);
		
		this.backgroundRefreshThreadFactory = new DefaultNamedThreadFactory("Background-Refresh-", " " + displayName, true);
		this.backgroundChangeResolverThreadFactory = new DefaultNamedThreadFactory("Background-Change-Resolver-", " " + displayName, true);
	}

	// implements Cached_TreeData 
	// ------------------------------------------------------------------------
	
	/**
	 * first query in 'cachedTree.get(path)'
	 * check if expiration delay not yet passed or if responseExceedTime>=now
	 * else call 'underlyingTree.get(path)'
	 * ... then updating cache with new result... and possibly resolve detected changes
	 * 
	 * example of changes detected:
	 * <PRE>
	 * - not in cache, but in underlying => insert in cache ... in background, recursive query underlying to insert in cache
	 * - in cache (expired), but not in underlying => delete from cache ... recursively on child + also search up to parent ancestor deletion + recursively delete
	 * - updated (refreshed cache) but not same list of childNames 
	 *      some child deleted => perform child remove() recursively
	 *      some child added => ... in background recursive query underlying to insert in cache
	 *                              ( may force query+put in cache the level=1 immediate children ?)
	 * </PRE>
	 */
	@Override
	public NodeData getCacheWaitMax(NodeNamesPath path, 
			int cacheExpirationMillis,
			long useCacheIfResponseExceedTimeMillis) {
		val now = System.currentTimeMillis();
		
		val cachedData = cachedTree.get(path);
		
		val endGetCacheTime = System.currentTimeMillis();
		int cachedGetMillis = (int) (endGetCacheTime - now);
		// cf next for incr cachedTree counter: either incrUsed() | incrCacheGetHitButExpired() | incrCacheGetMiss()
		
		if (cachedData != null) {
			// check cache expiration
			val cacheSinceMillis = now - cachedData.getLastExternalRefreshTimeMillis();
			if (cacheSinceMillis < cacheExpirationMillis  // fresh cached data
					|| endGetCacheTime > useCacheIfResponseExceedTimeMillis // not freshed enough, but accepted for response time 
					) {
				// OK fast path..
				incrCacheGetHit(cachedGetMillis, prefix -> log.info(prefix + " " + path)); 
				
				cachedData.incrUsed(cachedGetMillis);
				
				if (cacheSinceMillis > thresholdForBackgroundRefreshMillis
						&& startedBackgroupRefreshesSupport) {
					// may be refresh for later in background
					submitBackgroupRefreshIfStarted(path);
				}
				
				return cachedData;
			} else { // cachedData != null but expired 
				// got cached data, but considered expired!... need re-query from underlyingTree
				incrCacheGetHitButExpired(cachedGetMillis, prefix -> log.info(prefix + " " + path)); 
				NodeData newData;
				try {
				    // TODO counter
					if (underlyingTree_supportsPrefetchOther != null) {
						val prefetchCtx = createPrefetchContext(now, useCacheIfResponseExceedTimeMillis);
						newData = underlyingTree_supportsPrefetchOther.get(path, prefetchCtx);
					} else {
						newData = underlyingTree.get(path);
					}
					
				} catch(RuntimeException ex) {
					// failed to refresh from underlying, but still have an old cached data... 
					// may return it silently?!
					val endGetUnderlyingTime = System.currentTimeMillis();
					int underlyingGetMillis = (int) (endGetUnderlyingTime - endGetCacheTime);
					incrUnderlyingTreeGetFailed(underlyingGetMillis, prefix -> log.warn(prefix + " " + path + " " + ex.getMessage()));

					throw ex;
				}
				val endGetUnderlyingTime = System.currentTimeMillis();
				int underlyingGetMillis = (int) (endGetUnderlyingTime - endGetCacheTime);
				incrUnderlyingTreeGet(underlyingGetMillis, prefix -> log.info(prefix + " " + path));
				
				NodeData resData;
				if (newData != null) {
					// data exist, was in cache(expired)
					resData = copyWithLastExternalRefreshTimeMillis(newData, endGetCacheTime);
					
					// compare if changed from 'cachedData' to 'newData'
					boolean equalsIgnoreTransient = cachedData.equalsIgnoreTransientFields(newData);
					boolean equals = equalsIgnoreTransient && cachedData.compareTransientFields(newData);
					
					doCachePut_clearPendingTaskIfAny(path, resData, CachePutReason.underlyingGet, // TODO CachePutReason.underlyingGet_expired 
					        equalsIgnoreTransient, equals);

					if (!equalsIgnoreTransient) {
						// check compare+sync newData.childNames with previously cached
						resyncCacheChildListOf(path, resData, cachedData);
					}
					
				} else {
					// newData does not exist, but was in cache (expired)
					// => 'delete' event detected.. 
					doCacheRemove_clearPendingTaskIfAny(path);
					
					resyncDeletedAncestorOf(path);
					
					resData = null;
				}
				return resData;
			}
		} else {
			// cache miss
			incrCacheGetMiss(cachedGetMillis, prefix -> log.info(prefix + " " + path));
			
			// not-exist or not-in-cache? ... may save 'NotExistData' marker in cache?
			// no marker yet... when not-exist => will always cause cache miss

			NodeData newData;
			// TODO counter
			if (underlyingTree_supportsPrefetchOther != null) {
				val prefetchContext = createPrefetchContext(now, useCacheIfResponseExceedTimeMillis);
				
				newData = underlyingTree_supportsPrefetchOther.get(path, prefetchContext);
			} else {
				newData = underlyingTree.get(path);
			}
			
			val endGetUnderlyingTime = System.currentTimeMillis();
			int underlyingGetMillis = (int) (endGetUnderlyingTime - endGetCacheTime);
			incrUnderlyingTreeGet(underlyingGetMillis, prefix -> log.info(prefix + " " + path));
			
			NodeData resData;
			if (newData != null) {
				// newData exist but previously not in cache
				resData = copyWithLastExternalRefreshTimeMillis(newData, endGetCacheTime);
				
				doCachePut_clearPendingTaskIfAny(path, resData, CachePutReason.underlyingGet); // synonym underlyingGet_miss ? 

				// TODO does not work?... too slow, too many (repeated?) calls
//				// preload more child in background..
//				if (resData.childCount() > 0) {
//					for (val childName : resData.childNames) {
//						val childPath = path.toChild(childName);
//						enqueueRefresh_resolveChange(childPath);
//					}
//				}
				
			} else {
				// newData does not exist, and previously not in cache either
				// TOADD may save 'NotExistData' marker in cache?
				
				resData = null;
			}
			return resData;
		}
	}

	private LimitingPrefetchNodeDataContext createPrefetchContext(
			long fromStartTime, long maxTime) {
		val timeLimit = new PrefetchTimeLimit(prefetchLimitParams, fromStartTime, maxTime);
		val count = new PrefetchCount();
		return new LimitingPrefetchNodeDataContext(prefetchNodeDataCallback, timeLimit, count, 0);
	}

	// TOCHECK.. maybe useless?
	protected NodeData copyWithLastExternalRefreshTimeMillis(NodeData src, long timeMillis) {
		return new NodeData(src.name, src.type, src.mask, src.childNames, src.attrs, //
				src.externalCreationTime, src.externalLastModifiedTime, src.externalLength, //
				timeMillis, // lastExternalRefreshTimeMillis
				src.lastTreeDataUpdateTimeMillis, src.lastTreeDataUpdateCount, src.getTreeDataRecomputationMask(), // 
				1, 0, // src.lruCount, src.lruAmortizedCount, ... do not inherits lruCount from underlying!
				timeMillis // lastTreeDataQueryTimeMillis
				);
	}

   protected NodeData mergeIncompleteWithPrevData(NodeData src, NodeData prevData) {
       boolean isModif = src.externalLastModifiedTime > prevData.externalLastModifiedTime;
       long externalCreationTime = (isModif)? src.externalCreationTime : prevData.externalCreationTime;
       long externalLastModifiedTime = (isModif)? src.externalLastModifiedTime : prevData.externalLastModifiedTime; // idem max
       long externalLength = (isModif)? src.externalLength : prevData.externalLength;
       int lruCount = prevData.getLruCount();
       int lruAmortizedCount = prevData.getLruAmortizedCount();
       long lastTreeDataQueryTimeMillis = prevData.getLastTreeDataQueryTimeMillis(); // ?
       long lastExternalRefreshTimeMillis = Math.max(src.getLastExternalRefreshTimeMillis(), prevData.getLastExternalRefreshTimeMillis());
       return new NodeData(src.name, src.type, src.mask, src.childNames, src.attrs, //
               externalCreationTime, externalLastModifiedTime, externalLength, //
               lastExternalRefreshTimeMillis, //
               prevData.lastTreeDataUpdateTimeMillis, prevData.lastTreeDataUpdateCount, prevData.getTreeDataRecomputationMask(), // 
               lruCount, lruAmortizedCount, //
               lastTreeDataQueryTimeMillis);
    }

	   
	protected void resyncCacheChildListOf(NodeNamesPath path, NodeData data, NodeData prevCacheData) {
		LinkedHashSet<NodeName> remainToRemove = new LinkedHashSet<>(prevCacheData.childNames);
		List<NodeName> toAdd = new ArrayList<>();
		for(val childName: data.childNames) {
			boolean found = remainToRemove.remove(childName);
			if (! found) {
				toAdd.add(childName);
			}
		}
		if (! remainToRemove.isEmpty()) {
			for(val childName: remainToRemove) {
				val childPath = path.toChild(childName);
				doCacheRemove_clearPendingTaskIfAny(childPath);
			}
		}
		if (! toAdd.isEmpty()) {
			for(val childName: toAdd) {
				val childPath = path.toChild(childName);
				enqueueRefresh_resolveChange(childPath);
			}
		}
	}

	private void resyncDeletedAncestorOf(NodeNamesPath path) {
	    val putReason = CachePutReason.resyncDeletedAncestor; 
		// need resolve deleted ancestor parent, and recursive delete from cache
		// example: "a/b/c/d" detected as deleted .. check parent "a/b/c", then "a/b", then "a" ..
		val pathElementCount = path.pathElementCount(); // example 4
		if (pathElementCount > 0) {
			int ancestorPathLevel = pathElementCount-1; // example
			NodeNamesPath currAncestorPath = path.toParent();
			NodeData ancestorData = null;
			for(; ancestorPathLevel > 0; ancestorPathLevel--, currAncestorPath=currAncestorPath.toParent()) {
				
			    val underlyingGetBefore = System.currentTimeMillis();
			    
				// do not pass prefetchOtherCallback here (child of deleted parent?..)
				//.. instead of underlyingTree_supportsPrefetchOther.get(ancestorPath, prefetchOtherNodeDataCallback);
			    // TOADD counter
				ancestorData = underlyingTree.get(currAncestorPath);

				long underlyingGetMillis = System.currentTimeMillis() - underlyingGetBefore;
                
				val fCurrAncestorPath = currAncestorPath;
				underlyingTree_get_resyncDeletedAncestorCounter.incr(underlyingGetMillis, prefix -> log.info(prefix + " " + fCurrAncestorPath));
				
				if (ancestorData == null) {
					doCacheRemove_clearPendingTaskIfAny(currAncestorPath);
				} else {
					break;
				}
			}
			// loop stop either ancestorData==null or ancestorPathLevel==0
			val ancestorPath = currAncestorPath;
			
			val cacheGetBefore = System.currentTimeMillis();
			
			val prevCachedAncestorData = cachedTree.get(currAncestorPath);

			val cacheGetMillis = System.currentTimeMillis() - cacheGetBefore;
			cachedTree_get_resyncDeletedAncestorCounter.incr(cacheGetMillis, prefix -> log.info(prefix + " " + ancestorPath));
			
			if (ancestorData != null) {
				// update ancestor not removed
				doCachePut_clearPendingTaskIfAny(currAncestorPath, ancestorData, putReason);

				if (prevCachedAncestorData != null) {
					// => also check other previous cached ancestor-childs are in sync
					resyncCacheChildListOf(currAncestorPath, ancestorData, prevCachedAncestorData);
				} // else should not occur?
				
			} else { // ancestorPathLevel == 0
				// should not occur.. all ancestors deleted, even 'rootNode'?
				NodeNamesPath rootPath = NodeNamesPath.ROOT;
				val newRootData = new NodeData(path.pathElements[0], 0, 0,
						ImmutableSet.<NodeName>of(), ImmutableMap.<String,NodeAttr>of(),
						0L, 0, 0, 0, 0);
				doCachePut_clearPendingTaskIfAny(rootPath, newRootData, putReason);
			}
		}
	}


	// implements api Cached_TreeData
	// ------------------------------------------------------------------------
	
	@Override
	protected void doStartBackgroupRefreshSupport() {
		this.isRunningBackgroundRefresh = true;
		
		if (backgroundRefreshExecutorService == null) {
			getOrCreateBackgroundCacheExpiredRefreshExecutorService();
		}
	}

	@Override
	protected void doStopBackgroupRefreshSupport() {
		this.isRunningBackgroundRefresh = false;
		
		if (ownedBackgroundRefreshExecutorService) {
			val executorService = this.backgroundRefreshExecutorService;
			if (executorService != null) {
				this.backgroundRefreshExecutorService = null;
				this.ownedBackgroundRefreshExecutorService = false;
				log.info("ThreadPoolExecutor.shutdownNow() for Background-Refresh-* " + displayName);
				executorService.shutdownNow(); // pending tasks are returned.. will not be processed 
			}
		} // else do nothing?!
	}
	
	@Override
	// use CompletableFuture ?
	public Future<Void> submitBackgroupRefreshIfStarted(NodeNamesPath path) {
		if (! this.isRunningBackgroundRefresh) {
			// ignore do nothing
			return null;
		}
		Runnable refreshTask = () -> doRefresh(path);
		val executorService = backgroundRefreshExecutorService;
		if (executorService == null) {
			// should not occur.. 
			// ? still try process later after startBackgroupRefreshes()
			val runnableFuture = new FutureTask<Void>(refreshTask, null);
			backgroundRefreshQueue.add(runnableFuture);
			return runnableFuture;
		}
		
		@SuppressWarnings("unchecked")
		Future<Void> res = (Future<Void>) executorService.submit(refreshTask);
		return res;
	}

	@Override
	public void doRefresh(NodeNamesPath path) {
		int cacheExpirationMillis = 0;
		long useCacheIfResponseExceedTimeMillis = 0;
		getCacheWaitMax(path, cacheExpirationMillis, useCacheIfResponseExceedTimeMillis);
	}

	// ------------------------------------------------------------------------

	protected void safeOnPrefetchOtherNodeData(NodeNamesPath path, NodeData data, boolean isIncomplete) {
		val now = System.currentTimeMillis();
	    try {
			doOnPrefetchOtherNodeData(path, data, isIncomplete);
		} catch(Exception ex) {
			log.warn("Failed doOnPrefetchOtherNodeData .. ignore, no rethrow " + ex.getMessage());
		}
	    val millis = System.currentTimeMillis() - now;
	    underlying_prefetchedOtherNode_Counter.incr(millis, prefix -> log.info(prefix + " " + path));
	}

	protected void doOnPrefetchOtherNodeData(NodeNamesPath path, NodeData data, boolean isIncomplete) {
	    val putReason = CachePutReason.underlyingPrefetchOther;
	    val refreshTime = data.getLastExternalRefreshTimeMillis(); // TOCHECK
		NodeData newData = copyWithLastExternalRefreshTimeMillis(data, refreshTime);
		
		// check if previously already in cache, and with same time
		// if already in => do nothing
		// else => put in cache... but may enqueue refresh if still incomplete
		NodeData prevData = getIfInMemCache(path);

		if (prevData == null) {
			doCachePut_clearPendingTaskIfAny(path, newData, putReason);
		} else {
		    if (isIncomplete) {
		        // merge newData with existing prevData
		        newData = mergeIncompleteWithPrevData(newData, prevData);
		    }
			// compare if same time
			if (prevData.equalsIgnoreTransientFields(newData)) {
				// do nothing
			} else {
				if (!isIncomplete) {
					doCachePut_clearPendingTaskIfAny(path, newData, putReason);
				} else {
					// TOCHECK put in cache but still enqueue refresh later??
					doCachePut_clearPendingTaskIfAny(path, newData, putReason);
				}
			}
		}
		
	}

	private NodeData getIfInMemCache(NodeNamesPath path) {
		NodeData prevData;
		if (cachedTree_supportsInMemCached != null) {
			val prevDataIfInCache = cachedTree_supportsInMemCached.getIfInMemCache(path);
			if (prevDataIfInCache.status == OverrideNodeStatus.NOT_OVERRIDEN) {
				// not in-memory cached => cf next doCachePut_clearPendingTaskIfAny 
				prevData = null;
			} else {
				prevData = prevDataIfInCache.data;
			}
		} else {
			// may cause recurse!... get(parent) -> prefetchCallback(childPath, childData) -> get(childPath) !! 
		    // TOADD counter
		    prevData = cachedTree.get(path);
		}
		return prevData;
	}
	
	protected void safeOnProposePrefetchPathItem(PrefetchProposedPathItem proposedPathItem) {
		val path = proposedPathItem.getPath();
		// boolean accept = recurseLevel >= maxPrefetchCallbackLevel;
		// TODO TOADD
		
		// test not already in cache or cache.lastModifedTime != lastModifiedTime
		val foundCachedData = getIfInMemCache(path);
		
		boolean accept;
		if (foundCachedData == null) {
			accept = true;
		} else {
			// already in cache.. check for different lastModifiedTime
			accept = proposedPathItem.getLastModifiedTime() != foundCachedData.getExternalLastModifiedTime();
		}
		
		if (accept) {
			proposedPathItem.acceptToCompleteDataItem(prefetchNodeDataCallback);
		} else {
			// notify to maybe free memory
			proposedPathItem.ignoreToCompleteDataItem();
		}
	}

	
	
	// Update cache and manage Background Change Resolver / Pending Refresh Tasks
	// ------------------------------------------------------------------------

	protected void doCacheRemove_clearPendingTaskIfAny(NodeNamesPath path
			//, NodeData data
			) {
        val now = System.currentTimeMillis();
        
		cachedTree.remove(path);
		
		val millis = System.currentTimeMillis() - now;
		cachedTree_remove_Counter.incr(millis, prefix -> log.info(prefix + " " + path));
		clearPendingTaskIfAny(path);
	}
	
	protected void doCachePut_clearPendingTaskIfAny(NodeNamesPath path, NodeData data, CachePutReason putReason) {
		doCachePut_clearPendingTaskIfAny(path, data, putReason,
		        false, false);
	}

	protected void doCachePut_clearPendingTaskIfAny(NodeNamesPath path, NodeData data, CachePutReason putReason,
			boolean equalsIgnoreTransientFields, boolean equals) {
		if (! equals) {
		    val now = System.currentTimeMillis();
		    LoggingCounter counter;
		    
			if (! equalsIgnoreTransientFields) {
				cachedTree.put(path, data);
				
				switch(putReason) {
				case underlyingGet: counter = cachedTree_put_underlyingGet_Counter; break; 
				case underlyingGet_expired: counter = cachedTree_put_underlyingGet_expired_Counter; break; 
				case underlyingPrefetchOther: counter = cachedTree_put_underlyingPrefetchOther_Counter; break; 
				case resyncDeletedAncestor: counter = cachedTree_put_resyncDeletedAncestor_Counter; break; 
				default: // should not occur
				    counter = cachedTree_put_underlyingGet_Counter; break;
				}
			} else {
				cachedTree.put_transientFieldsChanged(path, data);
				
                switch(putReason) {
                case underlyingGet: counter = cachedTree_putTransient_underlyingGet_Counter; break; 
                case underlyingGet_expired: counter = cachedTree_putTransient_underlyingGet_expired_Counter; break; 
                case underlyingPrefetchOther: counter = cachedTree_putTransient_underlyingPrefetchOther_Counter; break; 
                case resyncDeletedAncestor: counter = cachedTree_putTransient_resyncDeletedAncestor_Counter; break; 
                default: // should not occur
                    counter = cachedTree_put_underlyingGet_Counter; break;
                }
			}

			val millis = System.currentTimeMillis() - now;
			counter.incr(millis, msgPrefix -> log.info(msgPrefix + " " + path));
		}
		clearPendingTaskIfAny(path);
	}
	
	protected void clearPendingTaskIfAny(NodeNamesPath path) {
		synchronized(pendingRefreshsLock) {
			PendingRefreshTask foundTask = pendingCacheExpiredTasks.remove(path);
			if (foundTask == null) {
				foundTask = pendingResolveChangeTasks.remove(path);
			}
			if (foundTask != null) {
				foundTask.status = PendingRefreshStatus.CANCEL_TASK_SKIP_REFRESH;
				if (foundTask.submitedFuture != null) {
					foundTask.submitedFuture.cancel(false);
					foundTask.submitedFuture = null;
				}
			}
		}
	}

	protected void enqueueRefresh_expired(NodeNamesPath path) {
		enqueueRefreshTask(new PendingRefreshTask(path, PendingRefreshStatus.REFRESH_CACHE_EXPIRED));
	}

	protected void enqueueRefresh_resolveChange(NodeNamesPath path) {
		enqueueRefreshTask(new PendingRefreshTask(path, PendingRefreshStatus.RESOLVE_CHANGE_DETECTED));
	}
	
	protected void enqueueRefreshTask(PendingRefreshTask task) {
		val path = task.path;
		val reason = task.status; // REFRESH_CACHE_EXPIRED or RESOLVE_CHANGE_DETECTED
		synchronized(pendingRefreshsLock) {
			PendingRefreshTask foundTask = pendingCacheExpiredTasks.get(path);
			if (foundTask == null) {
				foundTask = pendingResolveChangeTasks.get(path);
				if (foundTask == null) {
					// add to queue corresponding to reason
					if (reason == PendingRefreshStatus.REFRESH_CACHE_EXPIRED) {
						pendingCacheExpiredTasks.put(path, task);
					} else {
						pendingResolveChangeTasks.put(path, task);
					}
				} else { // not found
					if (foundTask.status == PendingRefreshStatus.RESOLVE_CHANGE_DETECTED) {
						// already queue.. do nothing
						return;
					} else { // if (foundTask.status != PendingRefreshStatus.RESOLVE_CHANGE_DETECTED) {
						foundTask.status = PendingRefreshStatus.RESOLVE_CHANGE_DETECTED;
					}
				}
			} else {
				if (foundTask.status == PendingRefreshStatus.REFRESH_CACHE_EXPIRED) {
					// already queue.. do nothing
					return;
				} else {
					// change queue
					pendingCacheExpiredTasks.remove(path);
					pendingResolveChangeTasks.put(path, foundTask);
				}
			}
		}
		
		// submit in ExecutorService if current inProgress < maxSubmitting
		if (reason == PendingRefreshStatus.REFRESH_CACHE_EXPIRED) {
			if (pendingCacheExpiredTaskProcessingCount < maxSubmittingPendingRefreshTaskCount) {
				val execService = getOrCreateBackgroundCacheExpiredRefreshExecutorService();
				execService.submit(() -> doProcessPendingRefreshTaskAndSubmitNext(task));
			}
		} else {
			if (pendingResolveChangeTaskProcessingCount < maxSubmittingResolveChangeTaskCount) {
				val execService = getOrCreateBackgroundChangedResolverExecutorService();
				execService.submit(() -> doProcessPendingChangedResolverTaskAndSubmitNext(task));
			}
			
		}
	}

	protected void doRefreshCache(NodeNamesPath path) {
		int cacheExpirationMillis = 5 * 60_000; // 5 minutes ... TODO TOCHECK !!! should enqueue path + minRefreshTime
		long useCacheIfResponseExceedTimeMillis = System.currentTimeMillis() + 100000;

        // TOADD counter

		getCacheWaitMax(path, cacheExpirationMillis, useCacheIfResponseExceedTimeMillis);
	}
		
	private void doProcessPendingRefreshTaskAndSubmitNext(PendingRefreshTask task) {
		boolean proceed;
		synchronized(pendingRefreshsLock) {
			pendingCacheExpiredTaskProcessingCount++;
			pendingCacheExpiredTaskSubmittedCount--;
			pendingCacheExpiredTasks.remove(task.path);
			proceed = !(task.status == PendingRefreshStatus.CANCEL_TASK_SKIP_REFRESH
					|| task.status == PendingRefreshStatus.IN_PROGRESS // should not occur
					); 
			task.status = PendingRefreshStatus.IN_PROGRESS;
		}
		try {
			if (proceed) {
				doRefreshCache(task.path);
			}
		} catch(Exception ex) {
			log.error("Failed proceed pending refresh task.. ignore? re-submit later?", ex);
		} finally {
			synchronized(pendingRefreshsLock) {
				pendingCacheExpiredTaskProcessingCount--;
				int totalPending = pendingCacheExpiredTasks.size();
				int countNotSubmittedYet = totalPending - pendingCacheExpiredTaskProcessingCount - pendingCacheExpiredTaskSubmittedCount;
				if (countNotSubmittedYet > 0) {
					// peek up next (not submitted yet) pending task to submit, by linked priority
				    val execService = getOrCreateBackgroundCacheExpiredRefreshExecutorService();
					for(val nextSubmitTask: pendingCacheExpiredTasks.values()) {
						if (nextSubmitTask.submitedFuture == null) {
						    // TOCHECK 
						    // nextSubmitTask.status = 
							nextSubmitTask.submitedFuture = execService.submit(() -> doProcessPendingRefreshTaskAndSubmitNext(nextSubmitTask));
							pendingCacheExpiredTaskSubmittedCount++;
							break;
						}
					}
				}
			}
		}
	}

	private void doProcessPendingChangedResolverTaskAndSubmitNext(PendingRefreshTask task) {
		boolean proceed;
		synchronized(pendingRefreshsLock) {
			pendingResolveChangeTaskProcessingCount++;
			pendingResolveChangeTaskSubmittedCount--;
			pendingResolveChangeTasks.remove(task.path);
			proceed = !(task.status == PendingRefreshStatus.CANCEL_TASK_SKIP_REFRESH
					|| task.status == PendingRefreshStatus.IN_PROGRESS // should not occur
					); 
			task.status = PendingRefreshStatus.IN_PROGRESS;
		}
		try {
			if (proceed) {
				doRefreshCache(task.path);
			}
		} catch(Exception ex) {
			log.error("Failed proceed pending changed resolver task.. ignore? re-submit later?", ex);
		} finally {
			synchronized(pendingRefreshsLock) {
				pendingResolveChangeTaskProcessingCount--;
				int totalPending = pendingResolveChangeTasks.size();
				int countNotSubmittedYet = totalPending - pendingResolveChangeTaskProcessingCount - pendingResolveChangeTaskSubmittedCount;
				if (countNotSubmittedYet > 0) {
					// peek up next (not submitted yet) pending task to submit, by linked priority
					for(val nextSubmitTask: pendingResolveChangeTasks.values()) {
						if (nextSubmitTask.submitedFuture == null) {
							val execService = getOrCreateBackgroundChangedResolverExecutorService();
							nextSubmitTask.submitedFuture = execService.submit(() -> doProcessPendingChangedResolverTaskAndSubmitNext(nextSubmitTask));
							pendingResolveChangeTaskSubmittedCount++;
							break;
						}
					}
				}
			}
		}
	}
	
	private ExecutorService getOrCreateBackgroundCacheExpiredRefreshExecutorService() {
		ExecutorService res = backgroundRefreshExecutorService;
		if (res == null) {
			int coreSize = 0; // core pool size .. so scaling to 0 when not needed
			int maxSize = 1;
			log.info("create ThreadPoolExecutor(core=" + coreSize + ", max=" + maxSize + "..) for Background-Refresh-* " + displayName);
			this.ownedBackgroundRefreshExecutorService = true;
			this.backgroundRefreshExecutorService = new ThreadPoolExecutor(coreSize, maxSize, //
	                60L, TimeUnit.SECONDS, // keepAliveTimeout
	                backgroundRefreshQueue,
	                backgroundRefreshThreadFactory);
		}
		return res;
	}
	
	private ExecutorService getOrCreateBackgroundChangedResolverExecutorService() {
		ExecutorService res = backgroundChangeResolverExecutorService;
		if (res == null) {
			if (useCommonForkJoinPoolForBackgroundChangedResolver) {
				res = ForkJoinPool.commonPool();
			} else {
				int coreSize = 0; // core pool size .. so scaling to 0 when not needed
				int maxSize = 10;
				log.info("create ThreadPoolExecutor(core=" + coreSize + ", max=" + maxSize + "..) for Background-Change-Resolver-* " + displayName);
				this.ownedBackgroundRefreshExecutorService = true;
				this.backgroundRefreshExecutorService = new ThreadPoolExecutor(coreSize, maxSize, //
		                60L, TimeUnit.SECONDS, // keepAliveTimeout
		                new SynchronousQueue<>(),
		                backgroundChangeResolverThreadFactory);
			}
		}
		return res;
	}

    @Override
    public void setLoggingCountersFreq(int freq) {
        super.setLoggingCountersFreq(freq);
        underlyingTree_get_resyncDeletedAncestorCounter.setLogFreq(freq);
        underlying_prefetchedOtherNode_Counter.setLogFreq(freq);

        cachedTree_put_underlyingGet_Counter.setLogFreq(freq);
        cachedTree_put_underlyingGet_expired_Counter.setLogFreq(freq);
        cachedTree_put_underlyingPrefetchOther_Counter.setLogFreq(freq);
        cachedTree_put_resyncDeletedAncestor_Counter.setLogFreq(freq);

        cachedTree_putTransient_underlyingGet_Counter.setLogFreq(freq);
        cachedTree_putTransient_underlyingGet_expired_Counter.setLogFreq(freq);
        cachedTree_putTransient_underlyingPrefetchOther_Counter.setLogFreq(freq);
        cachedTree_putTransient_resyncDeletedAncestor_Counter.setLogFreq(freq);

        cachedTree_remove_Counter.setLogFreq(freq);
        cachedTree_get_resyncDeletedAncestorCounter.setLogFreq(freq);
    }

    // TODO change to applyConfigureOnCounters(Consumer<> configureFunc)
//    @Override
//    public void setLoggingCountersMaxDelayMillis(int millis) {
//        super.setLoggingCountersMaxDelayMillis(millis);
//        underlyingTree_get_resyncDeletedAncestorCounter.setLogMaxDelayMillis(millis);
//        underlying_prefetchedOtherNode_Counter.setLogMaxDelayMillis(millis);
//        cachedTree_put_Counter.setLogMaxDelayMillis(millis);
//        cachedTree_remove_Counter.setLogMaxDelayMillis(millis);
//        cachedTree_put_transienFieldsChange_Counter.setLogMaxDelayMillis(millis);
//        cachedTree_get_resyncDeletedAncestorCounter.setLogMaxDelayMillis(millis);
//    }
	
}
