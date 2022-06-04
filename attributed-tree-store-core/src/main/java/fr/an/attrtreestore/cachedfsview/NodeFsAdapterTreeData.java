package fr.an.attrtreestore.cachedfsview;

import java.util.TreeSet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class NodeFsAdapterTreeData extends TreeData implements IReadTreeData {

	protected final NodeFsDataProvider delegate;

	// ------------------------------------------------------------------------
	
	@Override
	public NodeData get(NodeNamesPath path) {
		long timeBeforeMillis = System.currentTimeMillis(); // time client-side, before querying (otherwise may miss updates)

		NodeFsData fsData = delegate.queryNodeFsData(path);
		if (fsData == null) {
			return null;
		}
		// convert to NodeData
		val name = path.lastName();
		
		/** use-defined type... example: file / node / .. */
		int type;
		ImmutableSet<NodeName> childNames; 
		long externalLength;
		if (fsData instanceof DirNodeFsData) {
			val dir = (DirNodeFsData) fsData;
			type = NodeData.TYPE_DIR;
			externalLength = 0;
			val sortedNames = new TreeSet<>(dir.getChildEntries().keySet()); // redundant?
			childNames = ImmutableSet.copyOf(sortedNames);
		} else {
			val file = (FileNodeFsData) fsData;
			type = NodeData.TYPE_FILE;
			externalLength = file.fileLength;
			childNames = ImmutableSet.of();
		}
		int mask = 0;
		ImmutableMap<String,NodeAttr> attrs = ImmutableMap.of(); // TOADD?
		long externalCreationTime = fsData.creationTime;
		long externalLastModifiedTime = fsData.lastModifiedTime;
		long lastTreeDataUpdateTimeMillis = 0;
		int lastTreeDataUpdateCount = 0;

		// long lastTreeDataQueryTimeMillis = lastExternalRefreshTimeMillis;
		val res = new NodeData(name, type, mask, childNames, attrs, //
				externalCreationTime, externalLastModifiedTime, externalLength, //
				lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount);

		res.setLastExternalRefreshTimeMillis(timeBeforeMillis);
		res.setLastTreeDataQueryTimeMillis(timeBeforeMillis);
		return res;
	}
	
	
}
