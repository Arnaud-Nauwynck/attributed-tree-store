package fr.an.attrtreestore.util.fsdata;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;

public abstract class NodeFsDataVisitor {
	
	public abstract void caseFile(NodeNamesPath path, FileNodeFsData node);
	
	public abstract void caseDir(NodeNamesPath path, DirNodeFsData node);

}