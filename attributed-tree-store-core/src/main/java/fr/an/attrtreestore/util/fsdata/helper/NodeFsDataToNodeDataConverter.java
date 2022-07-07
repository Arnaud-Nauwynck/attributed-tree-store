package fr.an.attrtreestore.util.fsdata.helper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.RequiredArgsConstructor;
import lombok.val;

// TODO deprecated??? cf fr.an.attrtreestore.cachedfsview.converter.NodeFsDataToNodeDataConverter
@RequiredArgsConstructor
public class NodeFsDataToNodeDataConverter {
	
	private final long lastExternalRefreshTimeMillis; //  = System.currentTimeMillis(); // must take timestamp before querying..
	
	public NodeData toNodeData(DirNodeFsData src) {
		val childNames = ImmutableSet.copyOf(src.childNames); 
		return toNodeData(src, 1, 0L, childNames);
	}
	
	public NodeData toNodeData(FileNodeFsData src) {
		return toNodeData(src, 2, src.fileLength, ImmutableSet.<NodeName>of());
	}
	
	protected NodeData toNodeData(NodeFsData src,
			int type, //
			long externalLength, // specific for file
			ImmutableSet<NodeName> childNames // specific for dirs
			) {
		val name = src.name;
		int mask = 0;
		ImmutableMap<String,NodeAttr> attrs = ImmutableMap.<String,NodeAttr>of(); // TOADD src.attrs 
		
		long externalCreationTime = src.getCreationTime();
		long externalLastModifiedTime = src.getLastModifiedTime();
		
		long lastExternalRefreshTimeMillis = this.lastExternalRefreshTimeMillis;
		long lastTreeDataUpdateTimeMillis = 0;
		int lastTreeDataUpdateCount = 0;
		int treeDataRecomputationMask = 0;
		int lruCount = 0;
		int lruAmortizedCount = 0;
		long lastTreeDataQueryTimeMillis = 0;
		
		return new NodeData(name, type, mask, childNames, attrs, //
				externalCreationTime, externalLastModifiedTime, externalLength, //
				lastExternalRefreshTimeMillis, lastTreeDataUpdateTimeMillis, //
				lastTreeDataUpdateCount, treeDataRecomputationMask, //
				lruCount, lruAmortizedCount, lastTreeDataQueryTimeMillis);
	}

	
}
