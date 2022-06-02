package fr.an.attrtreestore.impl.traversal;

import java.util.LinkedHashSet;
import java.util.function.BiPredicate;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataDiffVisitor;
import fr.an.attrtreestore.api.traversal.TreeDataComparer;
import lombok.val;

/**
 * default implementation of TreeDataComparer
 */
public class DefaultTreeDataComparer<TCtx> extends TreeDataComparer<TCtx> {
	
	// ------------------------------------------------------------------------
	
	public DefaultTreeDataComparer(TreeData src, TreeData dest, //
			BiPredicate<NodeData, NodeData> compareDataFunc,
			NodeTreeDataDiffVisitor<TCtx> visitor
			) {
		super(src, dest, compareDataFunc, visitor);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void executeCompare() {
		val rootCtx = visitor.rootCtx();
		val rootPath = NodeNamesPath.ROOT;
		val srcData = src.get(rootPath);
		val prevDestData = dest.get(rootPath);
		if (srcData == null) {
			// should not occur
			recursiveRemove(rootPath, prevDestData, rootCtx);
			return;
		}
		if (prevDestData == null) {
			// should not occur
			recursiveAdd(rootPath, srcData, rootCtx);
			return;
		}
		
		recursiveCompare(rootPath, rootCtx);
	}
	
	public void recursiveCompare(NodeNamesPath currPath, TCtx ctx) {
		val srcData = src.get(currPath);
		assert srcData != null;
		
		val prevDestData = dest.get(currPath);
		// ensure prevDestData != null;  ???
		
		// compare data fields + child 
		boolean eqData = compareDataFunc.test(srcData, prevDestData);
		if (eqData) {
			countEq++;
			visitor.visitNodeDataEq(currPath, srcData, prevDestData, ctx);
		} else {
			countPutUpdate++;
			visitor.visitNodeDataUpdate(currPath, srcData, prevDestData, prevDestData, ctx);
		}
		// step 1: scan srcChild, mark remainingDestChildNames
		val remainDestChildNames = (prevDestData.childNames != null)? new LinkedHashSet<>(prevDestData.childNames) : new LinkedHashSet<NodeName>(); 
		val srcChildNames = srcData.childNames;
		if ((srcChildNames != null && !srcChildNames.isEmpty()) || !remainDestChildNames.isEmpty()) {
			val childListCtx = visitor.preVisitChildrenList(currPath, ctx);
			
			if (srcChildNames != null && !srcChildNames.isEmpty()) {
				for(val srcChildName: srcChildNames) {
					val childPath = currPath.toChild(srcChildName);
					boolean foundDestChild = remainDestChildNames.remove(srcChildName);
					if (foundDestChild) {
						recursiveCompare(childPath, childListCtx);
					} else {
						val childSrcData = src.get(childPath);
						recursiveAdd(childPath, childSrcData, childListCtx);
					}
				}
			}
			// step 2: scan remainingDestChildNames
			if (! remainDestChildNames.isEmpty()) {
				for(val childName: remainDestChildNames) {
					val childPath = currPath.toChild(childName);
					val destChildData = dest.get(childPath);
					recursiveRemove(childPath, destChildData, childListCtx);
				}
			}
			
			visitor.postVisitChildrenList(currPath, childListCtx);
		}
	}
	
	protected void recursiveAdd(NodeNamesPath currPath, NodeData srcData, TCtx ctx) {
		countPutAdd++;
		visitor.visitNodeDataAdd(currPath, srcData, ctx);

		// recurse..
		val srcChildNames = srcData.childNames;
		if ((srcChildNames != null && !srcChildNames.isEmpty())) {
			val childListCtx = visitor.preVisitChildrenList(currPath, ctx);
			
			for(val srcChildName: srcChildNames) {
				val childPath = currPath.toChild(srcChildName);
				val childSrcData = src.get(childPath);
				recursiveAdd(childPath, childSrcData, childListCtx);
			}

			visitor.postVisitChildrenList(currPath, childListCtx);
		}
	}
	
	protected void recursiveRemove(NodeNamesPath currPath, NodeData destData, TCtx ctx) {
		// recurse ... may also remove sub-child before.. but unnecessary (delete sub-tree is atomic)
		countRemove++;
		visitor.visitNodeDataRemove(currPath, destData, ctx);
	}
	
}
