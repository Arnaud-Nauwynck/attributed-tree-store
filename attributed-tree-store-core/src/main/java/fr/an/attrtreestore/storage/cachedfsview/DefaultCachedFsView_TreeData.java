package fr.an.attrtreestore.storage.cachedfsview;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.cachedfsview.CachedFsView_TreeData;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProvider;
import fr.an.attrtreestore.util.NotImplYet;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultCachedFsView_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends CachedFsView_TreeData<TCacheStorageTreeData> {

	private final ThreadFactory backgroundRefreshThreadFactory;
	private final BlockingQueue<Runnable> backgroundRefreshQueue = new SynchronousQueue<Runnable>();
	// ?? could be a PrioritySet / Map / ?? protected Set<NodeNamesPath> pendingBackgroupRefreshes;
	protected ThreadPoolExecutor backgroundRefreshExecutorService;

	private final ThreadFactory backgroundChangeResolverThreadFactory;
	protected ThreadPoolExecutor backgroundChangeResolverExecutorService;

	
	// ------------------------------------------------------------------------

	public DefaultCachedFsView_TreeData(
			String displayName,
			String displayBaseUrl,
			NodeFsDataProvider fsDataProvider, 
			TCacheStorageTreeData cachedTree) {
		super(displayName, displayBaseUrl, fsDataProvider, cachedTree);
		
		this.backgroundRefreshThreadFactory = new ThreadFactory() {
			int count = 0;
			@Override
			public Thread newThread(Runnable r) {
				int threadNum = count++;
				val res = new Thread("Background-Refresh-" + threadNum + " " + displayName);
				res.setDaemon(true);
				return res;
			}
		};
		this.backgroundChangeResolverThreadFactory = new ThreadFactory() {
			int count = 0;
			@Override
			public Thread newThread(Runnable r) {
				int threadNum = count++;
				val res = new Thread("Background-Conflict-Resolver-" + threadNum + " " + displayName);
				res.setDaemon(true);
				return res;
			}
		};  
		
	}

	// implements CachedFsView_TreeData 
	// ------------------------------------------------------------------------
	
	/**
	 * when querying cached FileSystem view
	 * <PRE>
	 * => check if data is in cache
	 *   when in cache => 
	 *   	=> check if expiration delay not yet passed  
	 *         otherwise if responseExceedTime>=now
	 *         => if ok, return cache ... no (immediate) check underlying Fs
	 *            ... may submitBackgroupRefresh
	 *         => else if not ok .. do not use cache, force immediate reload from underlying Fs
	 *            put in cache
	 *               => check for reconcile changes with previous cache
	 *                  if detected delete 
	 *                      ... also check parent dir for possible deletion
	 *                      update parent data of changed ancestor 
	 *                  	=> do apply recursive delete (all child from deleted ancestor)
	 *                  	+ update parent data of deleted ancestor
	 *                  if detected insert 
	 *                      also check parent dir for possible insertion
	 *                      do apply immediate insertion from parent...up to this node 
	 *                      submit async loading insertion
	 *                      update parent data of changed ancestor 
	 *                  if lastModifTime changed => update data
	 *                  if any changed detected
	 *                     => .. also check for possible change in parent  
	 *            return result
	 *   when not in cache
	 *      => do query underlying Fs
	 *        => check for reconcile changes with previous parents cached if any
	 *        if fs result data exist 
	 *           => submit async loading insertion
	 *        if fs result data not exist
	 *           => not in cache and not in fs either ... ok 
	 *           .. also check parent ? 
	 * </PRE>  
	 */
	@Override
	public NodeData getCacheWaitMax(NodeNamesPath path, 
			int cacheExpiryMinutes,
			long useCacheIfResponseExceedTimeMillis) {

		// TODO NOT IMPL
		throw NotImplYet.throw_TODO();

	}

	
	// implements api CachedFsView_TreeData
	// ------------------------------------------------------------------------
	
	@Override
	protected void doStartBackgroupRefreshes() {
		if (backgroundRefreshExecutorService == null) {
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
	protected void doStopBackgroupRefreshes() {
		val executorService = this.backgroundRefreshExecutorService;
		if (executorService != null) {
			this.backgroundRefreshExecutorService = null;
			log.info("ThreadPoolExecutor.shutdownNow() for Background-Refresh-* " + displayName);
			executorService.shutdownNow(); // pending tasks are returned.. will not be processed 
		}
	}
	
	@Override
	// use CompletableFuture ?
	public Future<NodeFsData> submitBackgroupRefresh(NodeNamesPath path) {
		Callable<NodeFsData> refreshCallable = () -> doRefresh(path);
		val executorService = backgroundRefreshExecutorService;
		if (executorService != null) {
			return executorService.submit(refreshCallable);
		} else {
			// ?? should not occur?
			// will be processed later after startBackgroupRefreshes()
			// convert Callable -> Runnable.. cf AbstractExecutorService.newTaskFor() ... catch(Throwable) { setException() }
			val runnableFuture = new FutureTask<>(refreshCallable);
			backgroundRefreshQueue.add(runnableFuture);
			return runnableFuture;
		}
	}

	public NodeFsData doRefresh(NodeNamesPath path) {
		// TODO NOT IMPL
		throw NotImplYet.throw_TODO();
	}

	// ------------------------------------------------------------------------
	
}
