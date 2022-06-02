package fr.an.attrtreestore.impl.traversal;

import java.util.LinkedHashSet;
import java.util.function.BiPredicate;
import java.util.function.Function;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataDiffVisitor;
import fr.an.attrtreestore.api.traversal.TreeDataMirrorCopier;
import lombok.val;

/**
 * default implementation of TreeDataMirrorCopier
 */
public class DefaultTreeDataMirrorCopier<TCtx> extends TreeDataMirrorCopier<TCtx> {
	
	// ------------------------------------------------------------------------
	
	public DefaultTreeDataMirrorCopier(TreeData src, IWriteTreeData dest, //
			BiPredicate<NodeData, NodeData> compareDataFunc, //
			Function<NodeData, NodeData> copyDataFunc, //
			NodeTreeDataDiffVisitor<TCtx> visitor
			) {
		super(src, dest, compareDataFunc, copyDataFunc, visitor);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void executeMirrorCopyTreeData() {
		val rootCtx = visitor.rootCtx();
		val rootPath = NodeNamesPath.ROOT;
		val srcData = src.get(rootPath);
		val prevDestData = dest.get(rootPath);
		if (srcData == null) {
			// should not occur
			countRemove++;
			visitor.visitNodeDataRemove(rootPath, prevDestData, rootCtx);
			return;
		}
		if (prevDestData == null) {
			// should not occur
			countPutAdd++;
			visitor.visitNodeDataAdd(rootPath, srcData, rootCtx);
			return;
		}
		
		recursiveMirrorCopy(rootPath, rootCtx);
	}
	
	public void recursiveMirrorCopy(NodeNamesPath currPath, TCtx ctx) {
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
			val destData = copyDataFunc.apply(srcData);
			dest.put(currPath, destData);
			visitor.visitNodeDataUpdate(currPath, srcData, prevDestData, destData, ctx);
		}

		val remainDestChildNames = (prevDestData.childNames != null)? new LinkedHashSet<>(prevDestData.childNames) : new LinkedHashSet<NodeName>(); 
		val srcChildNames = srcData.childNames;
		if ((srcChildNames != null && !srcChildNames.isEmpty()) || !remainDestChildNames.isEmpty()) {
			val childListCtx = visitor.preVisitChildrenList(currPath, ctx);
		
			// step 1: scan srcChild, mark remainingDestChildNames
			if (srcChildNames != null && !srcChildNames.isEmpty()) {
				for(val srcChildName: srcChildNames) {
					val childPath = currPath.toChild(srcChildName);
					boolean foundDestChild = remainDestChildNames.remove(srcChildName);
					if (foundDestChild) {
						recursiveMirrorCopy(childPath, childListCtx);
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
					val childDestData = dest.get(childPath);
					recursiveRemove(childPath, childDestData, childListCtx);
				}
			}
			
			visitor.postVisitChildrenList(currPath, childListCtx);
		}
	}

	protected void recursiveAdd(NodeNamesPath currPath, NodeData srcData, TCtx ctx) {
		countPutAdd++;
		val destData = copyDataFunc.apply(srcData);
		dest.put(currPath, destData);
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
		dest.remove(currPath);

		countRemove++;
		visitor.visitNodeDataRemove(currPath, destData, ctx);
	}
	
}
