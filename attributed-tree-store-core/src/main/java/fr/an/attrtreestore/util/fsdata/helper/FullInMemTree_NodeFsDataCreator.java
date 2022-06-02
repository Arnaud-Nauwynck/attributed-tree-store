package fr.an.attrtreestore.util.fsdata.helper;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.storage.impl.InMem_TreeData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsDataVisitor;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class FullInMemTree_NodeFsDataCreator extends NodeFsDataVisitor {

	private final NodeFsDataToNodeDataConverter converter;
	private final InMem_TreeData tree;
	
	@Override
	public void caseFile(NodeNamesPath path, FileNodeFsData fsData) {
		val nodeData = converter.toNodeData(fsData);
		tree.put(path, nodeData);
	}

	@Override
	public void caseDir(NodeNamesPath path, DirNodeFsData fsData) {
		val nodeData = converter.toNodeData(fsData);
		tree.put(path, nodeData);
	}

}
