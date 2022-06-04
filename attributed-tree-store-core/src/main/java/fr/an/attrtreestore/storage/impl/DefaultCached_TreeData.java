package fr.an.attrtreestore.storage.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.RefreshableCached_TreeData;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.util.DefaultNamedTreadFactory;
import fr.an.attrtreestore.util.NotImplYet;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class DefaultCached_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends RefreshableCached_TreeData<TCacheStorageTreeData> {

	private final ThreadFactory backgroundRefreshThreadFactory;
	private final BlockingQueue<Runnable> backgroundRefreshQueue = new SynchronousQueue<Runnable>();
	// ?? could be a PrioritySet / Map / ?? protected Set<NodeNamesPath> pendingBackgroupRefreshes;
	protected boolean isRunningBackgroundRefresh;
	protected boolean ownedBackgroundRefreshExecutorService;
	// ExecutorService, may be shared, otherwise created on demand and marked as owned for later shutdown()
	protected ExecutorService backgroundRefreshExecutorService;

	private final ThreadFactory backgroundChangeResolverThreadFactory;
	protected boolean ownedBackgroundChangedResolverExecutorService;
	// ExecutorService, may be shared, otherwise created on demand and marked as owned for later shutdown()
	protected ExecutorService backgroundChangeResolverExecutorService;
	
	
	// ------------------------------------------------------------------------

	public DefaultCached_TreeData(
			String displayName,
			String displayBaseUrl,
			TreeData underlyingTree, 
			TCacheStorageTreeData cachedTree) {
		super(displayName, displayBaseUrl, underlyingTree, cachedTree);
		
		this.backgroundRefreshThreadFactory = new DefaultNamedTreadFactory("Background-Refresh-", " " + displayName, true);
		this.backgroundChangeResolverThreadFactory = new DefaultNamedTreadFactory("Background-Conflict-Resolver-", " " + displayName, true);
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
		
		if (cachedData != null) {
			// check cache expiration
			val cacheSinceMillis = cachedData.getLastExternalRefreshTimeMillis();
			if (cacheSinceMillis < cacheExpirationMillis  // fresh cached data
					|| endGetCacheTime > useCacheIfResponseExceedTimeMillis // not freshed enough, but acceted for response time 
					) {
				// OK fast path..
				incrCacheGetHit(cachedGetMillis); 
				
				cachedData.incrUsed(cachedGetMillis);
				
				if (cacheSinceMillis > thresholdForBackgroundRefreshMillis
						&& startedBackgroupRefreshesSupport) {
					// may be refresh for later in background
					submitBackgroupRefreshIfStarted(path);
				}
				
				return cachedData;
			} else { // cachedData != null but expired 
				// got cached data, but considered expired!... need re-query from underlyingTree
				incrCacheGetHitButExpired(cachedGetMillis); 
				NodeData newData;
				try {
					newData = underlyingTree.get(path);
					
				} catch(RuntimeException ex) {
					// failed to refresh from underlying, but still have an old cached data... 
					// may return it silently?!
					val endGetUnderlyingTime = System.currentTimeMillis();
					int underlyingGetMillis = (int) (endGetUnderlyingTime - endGetCacheTime);
					incrUnderlyingTreeGetFailed(underlyingGetMillis, ex);

					throw ex;
				}
				val endGetUnderlyingTime = System.currentTimeMillis();
				int underlyingGetMillis = (int) (endGetUnderlyingTime - endGetCacheTime);
				incrUnderlyingTreeGet(underlyingGetMillis);
				
				NodeData resData;
				if (newData != null) {
					// data exist, was in cache(expired)
					// TODO
					resData = copyWithLastExternalRefreshTimeMillis(newData, endGetCacheTime);
					
					// compare if changed from 'cachedData' to 'newData'
					// TODO
					
					cachedTree.put(path, resData);
					// else cachedTree.put_transientFieldsChanged(path, cachedData);
					
					// also compare childNames... for detecting recursive insert/delete on child
					// TODO
					
				} else {
					// newData does not exist, but was in cache (expired)
					// => 'delete' event detected.. + need resolve recursively (child) + also check parent

					// TODO
					
					resData = null;
				}
				return resData;
			}
		} else {
			// cache miss
			incrCacheGetMiss(cachedGetMillis);
			
			// not-exist or not-in-cache? ... may save 'NotExistData' marker in cache?
			// no marker yet... when not-exist => will always cause cache miss
			
			val newData = underlyingTree.get(path);
			
			val endGetUnderlyingTime = System.currentTimeMillis();
			int underlyingGetMillis = (int) (endGetUnderlyingTime - endGetCacheTime);
			incrUnderlyingTreeGet(underlyingGetMillis);
			
			NodeData resData;
			if (newData != null) {
				// newData exist but previously not in cache
				resData = copyWithLastExternalRefreshTimeMillis(newData, endGetCacheTime);
				
				cachedTree.put(path, resData);

				// may preload more child in background..
				if (startedBackgroupRefreshesSupport && resData.childCount() > 0) {
					for (val childName : resData.childNames) {
						val childPath = path.toChild(childName);
						submitBackgroupRefreshIfStarted(childPath);
					}
				}
				
			} else {
				// newData does not exist, and previously not in cache either
				// TOADD may save 'NotExistData' marker in cache?
				
				resData = null;
			}
			return resData;
		}

	}

	protected NodeData copyWithLastExternalRefreshTimeMillis(NodeData src, long timeMillis) {
		return new NodeData(src.name, src.type, src.mask, src.childNames, src.attrs, //
				src.externalCreationTime, src.externalLastModifiedTime, src.externalLength, //
				timeMillis, // lastExternalRefreshTimeMillis
				src.lastTreeDataUpdateTimeMillis, src.lastTreeDataUpdateCount, src.getTreeDataRecomputationMask(), // 
				1, 0, // src.lruCount, src.lruAmortizedCount, ... do not inherits lruCount from underlying!
				timeMillis // lastTreeDataQueryTimeMillis
				);
	}
	
	// implements api Cached_TreeData
	// ------------------------------------------------------------------------
	
	@Override
	protected void doStartBackgroupRefreshSupport() {
		this.isRunningBackgroundRefresh = true;
		
		if (backgroundRefreshExecutorService == null) {
			this.ownedBackgroundRefreshExecutorService = true;
			log.info("create ThreadPoolExecutor(core=0, max=1..) for Background-Refresh-* " + displayName);
			this.backgroundRefreshExecutorService = new ThreadPoolExecutor( //
					0, // core pool size .. so scaling to 0 when not needed 
					1, // max pool size
	                60L, TimeUnit.SECONDS, // keepAliveTimeout
	                backgroundRefreshQueue,
	                backgroundRefreshThreadFactory);
		}
	}

	@Override
	protected void doStopBackgroupRefreshSupport() {
		this.isRunningBackgroundRefresh = false;
		
		if (ownedBackgroundRefreshExecutorService) {
			val executorService = this.backgroundRefreshExecutorService;
			if (executorService != null) {
				this.backgroundRefreshExecutorService = null;
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

	public void doRefresh(NodeNamesPath path) {
		// TODO NOT IMPL
		throw NotImplYet.throw_TODO();
	}

	// ------------------------------------------------------------------------
	
}
