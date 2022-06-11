package fr.an.attrtreestore.api.readprefetch;

import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * opaque object for proposing NodeData, before data is actually completed/requested to the backend
 *
 */
@AllArgsConstructor
@Getter
public abstract class PrefetchProposedPathItem {
	
	protected NodeNamesPath path; 
	protected int type;
	protected long lastModifiedTime; 
	protected long refreshTimeMillis;
	protected boolean forIncompleteData; // useless here?
	
	public abstract void acceptToCompleteDataItem(
			// can be set in field
			PrefetchNodeDataContext prefetchContext
			);

	public void ignoreToCompleteDataItem() {
	}

	
}