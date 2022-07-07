package fr.an.attrtreestore.cachedfsview.converter;

import java.util.TreeSet;

import javax.annotation.Nonnull;

import org.path4j.NodeName;
import org.path4j.NodeNamesPath;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.val;

public abstract class NodeFsDataToNodeDataConverter {
	
	// convert to NodeFsData -> NodeData
	public abstract NodeData nodeFsDataToNodeData(NodeNamesPath path, NodeFsData fsData, long refreshTimeMillis, boolean isIncomplete);
	
	// ------------------------------------------------------------------------
	
	public static class DefaultNodeFsDataToNodeDataConverter extends NodeFsDataToNodeDataConverter {

		public static final DefaultNodeFsDataToNodeDataConverter INSTANCE = new DefaultNodeFsDataToNodeDataConverter(); 
		
		@Override
		public NodeData nodeFsDataToNodeData(NodeNamesPath path, NodeFsData fsData, long refreshTimeMillis, boolean isIncomplete) {
			val name = fsData.name; // check caller .. path.lastNameOrEmpty();
			
			/** use-defined type... example: file / node / .. */
			int type;
			ImmutableSet<NodeName> childNames; 
			long externalLength;
			if (fsData instanceof DirNodeFsData) {
				val dir = (DirNodeFsData) fsData;
				type = NodeData.TYPE_DIR;
				externalLength = 0;
				TreeSet<NodeName> sortedNames = dir.getChildNames(); // already sorted TreeSet
				childNames = ImmutableSet.copyOf(sortedNames);
			} else {
				val file = (FileNodeFsData) fsData;
				type = NodeData.TYPE_FILE;
				externalLength = file.fileLength;
				childNames = ImmutableSet.of();
			}
			int mask = 0;
			ImmutableMap<String,NodeAttr> attrs = nodeFsDataToAttrs(path, fsData, refreshTimeMillis);
			long externalCreationTime = fsData.creationTime;
			long externalLastModifiedTime = fsData.lastModifiedTime;
			long lastTreeDataUpdateTimeMillis = 0;
			int lastTreeDataUpdateCount = 0;
			
			// long lastTreeDataQueryTimeMillis = lastExternalRefreshTimeMillis;
			val res = new NodeData(name, type, mask, childNames, attrs, //
					externalCreationTime, externalLastModifiedTime, externalLength, //
					lastTreeDataUpdateTimeMillis, lastTreeDataUpdateCount);
			
			res.setLastExternalRefreshTimeMillis(refreshTimeMillis);
			res.setLastTreeDataQueryTimeMillis(refreshTimeMillis);
			return res;
		}

		// overridable
		protected @Nonnull ImmutableMap<String, NodeAttr> nodeFsDataToAttrs(NodeNamesPath path, NodeFsData fsData, long refreshTimeMillis) {
			return ImmutableMap.of();
		}
		
	}

}
