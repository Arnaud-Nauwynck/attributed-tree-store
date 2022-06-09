package fr.an.attrtreestore.cachedfsview;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.util.fsdata.NodeFsData;

public abstract class PrefetchOtherNodeFsDataCallback {
	
	public abstract void onPrefetchOtherNodeFsData(NodeNamesPath path, NodeFsData data, long refreshTimeMillis, boolean isIncomplete);
		
}
