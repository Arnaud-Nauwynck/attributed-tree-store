package fr.an.attrtreestore.impl.traversal;

import java.util.LinkedHashSet;
import java.util.function.BiPredicate;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.TreeDataComparer;
import lombok.val;

/**
 * default implementation of TreeDataComparer
 */
public class DefaultTreeDataComparer extends TreeDataComparer {
	
	// ------------------------------------------------------------------------
	
	public DefaultTreeDataComparer(TreeData src, TreeData dest, //
			BiPredicate<NodeData, NodeData> compareDataFunc
			) {
		super(src, dest, compareDataFunc);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void executeCompare() {
		val rootPath = NodeNamesPath.ROOT;
		val srcData = src.get(rootPath);
		val prevDestData = dest.get(rootPath);
		if (srcData == null) {
			// should not occur
			addDiffRemove(rootPath, prevDestData);
			return;
		}
		if (prevDestData == null) {
			// should not occur
			addDiffAdd(rootPath, srcData);
			return;
		}
		
		recursiveCompare(rootPath);
	}
	
	public void recursiveCompare(NodeNamesPath currPath) {
		val srcData = src.get(currPath);
		assert srcData != null;
		
		val prevDestData = dest.get(currPath);
		// ensure prevDestData != null;  ???
		
		// compare data fields + child 
		boolean eqData = compareDataFunc.test(srcData, prevDestData);
		if (!eqData) {
			addDiffUpdate(currPath, srcData, prevDestData);
		}
		// step 1: scan srcChild, mark remainingDestChildNames
		val remainDestChildNames = (prevDestData.childNames != null)? new LinkedHashSet<>(prevDestData.childNames) : new LinkedHashSet<NodeName>(); 
		val srcChildNames = srcData.childNames;
		if (srcChildNames != null && !srcChildNames.isEmpty()) {
			for(val srcChildName: srcChildNames) {
				val childPath = currPath.toChild(srcChildName);
				boolean foundDestChild = remainDestChildNames.remove(srcChildName);
				if (foundDestChild) {
					recursiveCompare(childPath);
				} else {
					val childSrcData = src.get(childPath);
					addDiffAdd(childPath, childSrcData);
				}
			}
		}
		// step 2: scan remainingDestChildNames
		if (! remainDestChildNames.isEmpty()) {
			for(val childName: remainDestChildNames) {
				val childPath = currPath.toChild(childName);
				val destChildData = dest.get(childPath);
				addDiffRemove(childPath, destChildData);
			}
		}
	}

	protected void addDiffAdd(NodeNamesPath path, NodeData srcData) {
		countPutAdd++;
	}

	protected void addDiffUpdate(NodeNamesPath path, NodeData srcData, NodeData destData) {
		countPutUpdate++;
	}

	protected void addDiffRemove(NodeNamesPath currPath, NodeData destData) {
		countRemove++;
	}

}
