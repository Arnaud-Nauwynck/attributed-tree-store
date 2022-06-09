package fr.an.attrtreestore.cachedfsview;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.PrefetchOtherNodeDataCallback;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class ConverterNodeFsDataCallback extends PrefetchOtherNodeFsDataCallback {
	
	private final PrefetchOtherNodeDataCallback delegate;
	private final NodeFsDataToNodeDataConverter nodeDataConverter;
	
	@Override
	public void onPrefetchOtherNodeFsData(NodeNamesPath path, NodeFsData data, long refreshTimeMillis, boolean isIncomplete) {
		val nodeData = nodeDataConverter.nodeFsDataToNodeData(path, data, refreshTimeMillis, isIncomplete);
		try {
			delegate.onPrefetchOtherNodeData(path, nodeData, isIncomplete);
		} catch(Exception ex) {
			log.warn("Failed onPrefetchOtherNodeData '" + path + "'.. ignore " + ex.getMessage());
		}
	}
	
}