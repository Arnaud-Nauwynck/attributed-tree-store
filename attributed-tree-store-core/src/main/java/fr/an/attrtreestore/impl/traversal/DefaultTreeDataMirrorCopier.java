package fr.an.attrtreestore.impl.traversal;

import java.util.LinkedHashSet;
import java.util.function.BiPredicate;
import java.util.function.Function;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.TreeDataMirrorCopier;
import lombok.val;

/**
 * default implementation of TreeDataMirrorCopier
 */
public class DefaultTreeDataMirrorCopier extends TreeDataMirrorCopier {
	
	// ------------------------------------------------------------------------
	
	public DefaultTreeDataMirrorCopier(TreeData src, IWriteTreeData dest, //
			BiPredicate<NodeData, NodeData> compareDataFunc, //
			Function<NodeData, NodeData> copyDataFunc //
			) {
		super(src, dest, compareDataFunc, copyDataFunc);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void executeMirrorCopyTreeData() {
		val rootPath = NodeNamesPath.ROOT;
		val srcData = src.get(rootPath);
		val prevDestData = dest.get(rootPath);
		if (srcData == null) {
			// should not occur
			recursiveRemove(rootPath);
			return;
		}
		if (prevDestData == null) {
			// should not occur
			recursiveAdd(rootPath, srcData);
			return;
		}
		
		recursiveMirrorCopy(rootPath);
	}
	
	public void recursiveMirrorCopy(NodeNamesPath currPath) {
		val srcData = src.get(currPath);
		assert srcData != null;
		
		val prevDestData = dest.get(currPath);
		// ensure prevDestData != null;  ???
		
		// compare data fields + child 
		boolean eqData = compareDataFunc.test(srcData, prevDestData);
		if (!eqData) {
			val destData = copyDataFunc.apply(srcData);
			dest.put(currPath, destData);
			countPutUpdate++;
		}
		// step 1: scan srcChild, mark remainingDestChildNames
		val remainDestChildNames = (prevDestData.childNames != null)? new LinkedHashSet<>(prevDestData.childNames) : new LinkedHashSet<NodeName>(); 
		val srcChildNames = srcData.childNames;
		if (srcChildNames != null && !srcChildNames.isEmpty()) {
			for(val srcChildName: srcChildNames) {
				val childPath = currPath.toChild(srcChildName);
				boolean foundDestChild = remainDestChildNames.remove(srcChildName);
				if (foundDestChild) {
					recursiveMirrorCopy(childPath);
				} else {
					val childSrcData = src.get(childPath);
					recursiveAdd(childPath, childSrcData);
				}
			}
		}
		// step 2: scan remainingDestChildNames
		if (! remainDestChildNames.isEmpty()) {
			for(val childName: remainDestChildNames) {
				val childPath = currPath.toChild(childName);
				recursiveRemove(childPath);
			}
		}
	}

	protected void recursiveAdd(NodeNamesPath currPath, NodeData srcData) {
		val destData = copyDataFunc.apply(srcData);
		dest.put(currPath, destData);
		countPutAdd++;

		val srcChildNames = srcData.childNames;
		if (srcChildNames != null && !srcChildNames.isEmpty()) {
			for(val srcChildName: srcChildNames) {
				val childPath = currPath.toChild(srcChildName);
				val srcChildData = src.get(childPath);
				recursiveAdd(childPath, srcChildData);
			}
		}
	}
	
	protected void recursiveRemove(NodeNamesPath currPath) {
		dest.remove(currPath); // TreeData should be already recursive.. no need to iterate..
		countRemove++;
	}

}
