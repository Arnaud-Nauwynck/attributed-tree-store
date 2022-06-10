package fr.an.attrtreestore.api;

import java.util.concurrent.Future;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public abstract class SyncCachedImage_TreeData<TCacheStorageTreeData extends TreeData & IWriteTreeData> 
	extends CachedImage_TreeData<TCacheStorageTreeData> {

	@Getter @Setter
	protected long thresholdForBackgroundRefreshMillis = defaultCacheExpirationMillis - 1000; 
	

	// ------------------------------------------------------------------------

	public SyncCachedImage_TreeData(String displayName, String displayBaseUrl, //
			TreeData underlyingTree, 
			TCacheStorageTreeData cachedTree) {
		super(displayName, displayBaseUrl, underlyingTree, cachedTree);
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
