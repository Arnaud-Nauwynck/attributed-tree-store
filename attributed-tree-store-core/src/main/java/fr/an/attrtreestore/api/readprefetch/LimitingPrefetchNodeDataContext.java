package fr.an.attrtreestore.api.readprefetch;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.NodeData;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LimitingPrefetchNodeDataContext extends PrefetchNodeDataContext {
	
	@AllArgsConstructor
	public static class PrefetchLimitParams {
		public final int maxCount;
		public final int maxRecurseLevel;
		public final long maxDurationMillis;
	}
	
	public static class PrefetchTimeLimit {
		public final int maxCount;
		public final int maxRecurseLevel;
		public final long maxTimeMillis;
		
		public PrefetchTimeLimit(PrefetchLimitParams limitParams, long fromStartTime, long maxTime) {
			this.maxCount = limitParams.maxCount;
			this.maxRecurseLevel = limitParams.maxRecurseLevel;
			this.maxTimeMillis = (limitParams.maxDurationMillis >= 0)?
					Math.min(fromStartTime + limitParams.maxDurationMillis, maxTime) : maxTime;
		}
	}
	
	public static class PrefetchCount {
		private int currCount;
		public synchronized void incr() {
			currCount++;
		}
		public synchronized int getCount() {
			return currCount;
		}
		public synchronized boolean matchLimit(PrefetchTimeLimit limit) {
			return (-1 == limit.maxCount || currCount < limit.maxCount)
					&& (0 >= limit.maxTimeMillis || System.currentTimeMillis() < limit.maxTimeMillis);
		}
	}

	// ------------------------------------------------------------------------
	
	protected PrefetchNodeDataContext delegate;
	protected PrefetchTimeLimit prefetchLimit;
	protected PrefetchCount prefetchCount;
	protected int currRecurseLevel;

	// ------------------------------------------------------------------------
	
	public void incr() {
		prefetchCount.incr();
	}

	@Override
	public PrefetchNodeDataContext createChildPrefetchContext() {
		return createChildLimitingPrefetchContext();
	}
	
	public LimitingPrefetchNodeDataContext createChildLimitingPrefetchContext() {
		return new LimitingPrefetchNodeDataContext(delegate, prefetchLimit, prefetchCount, currRecurseLevel + 1);
	}
	
	@Override
	public boolean acceptPrefectNodeDatas() {
		return prefetchCount.matchLimit(prefetchLimit);
	}

	@Override
	public void onPrefetchNodeData(NodeNamesPath path, NodeData data, boolean isIncomplete) {
		delegate.onPrefetchNodeData(path, data, isIncomplete);
	}

	@Override
	public boolean acceptRecurseProposePrefectPathItems() {
		return prefetchCount.matchLimit(prefetchLimit)
				&& (-1 == prefetchLimit.maxRecurseLevel || currRecurseLevel + 1 < prefetchLimit.maxRecurseLevel);
	}

	@Override
	public void onProposePrefetchPathItem(PrefetchProposedPathItem prefetchDataItem) {
		delegate.onProposePrefetchPathItem(prefetchDataItem);
	}

}