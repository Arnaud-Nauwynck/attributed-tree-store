package fr.an.attrtreestore.util.fsdata.helper;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsDataVisitor;
import lombok.Getter;

public class NodeFsDataCounter extends NodeFsDataVisitor {
	
	@Getter
	protected int countFile;

	@Getter
	protected int countDir;


	@Override
	public void caseFile(NodeNamesPath path, FileNodeFsData node) {
		countFile++;
	}


	@Override
	public void caseDir(NodeNamesPath path, DirNodeFsData node) {
		countDir++;
	}
	
}
