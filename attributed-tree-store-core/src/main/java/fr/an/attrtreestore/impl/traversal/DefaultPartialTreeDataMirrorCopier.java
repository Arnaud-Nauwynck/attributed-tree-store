package fr.an.attrtreestore.impl.traversal;

import java.util.LinkedHashSet;
import java.util.function.BiPredicate;
import java.util.function.Function;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.partial.PartialNodeData;
import fr.an.attrtreestore.api.partial.PartialTreeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataPartialDiffVisitor;
import fr.an.attrtreestore.api.traversal.PartialTreeDataMirrorCopier;
import lombok.val;

/**
 * default implementation of TreeDataMirrorCopier
 */
public class DefaultPartialTreeDataMirrorCopier<TCtx> extends PartialTreeDataMirrorCopier<TCtx> {
	
	// ------------------------------------------------------------------------
	
	public DefaultPartialTreeDataMirrorCopier(PartialTreeData src, IWriteTreeData dest, //
			BiPredicate<NodeData, NodeData> compareDataFunc, //
			Function<NodeData, NodeData> copyDataFunc, //
			NodeTreeDataPartialDiffVisitor<TCtx> visitor
			) {
		super(src, dest, compareDataFunc, copyDataFunc, visitor);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void executeMirrorCopyTreeData() {
		val rootCtx = visitor.rootCtx();
		val rootPath = NodeNamesPath.ROOT;
		
		val srcPartialData = src.getPartial(rootPath);
		
		val prevDestData = dest.get(rootPath);
		if (srcPartialData == null) {
			// should not occur
			// countRemove++;
			// visitor.visitNodeDataRemove(rootPath, prevDestData, rootCtx);
			return;
		}
		if (prevDestData == null) {
			// should not occur
			countPutAdd++;
			visitor.visitNodeDataAdd(rootPath, srcPartialData, rootCtx);
			return;
		}
		
		recursiveMirrorCopy(rootPath, rootCtx);
	}
	
	public void recursiveMirrorCopy(NodeNamesPath currPath, TCtx ctx) {
		val srcPartialData = src.getPartial(currPath);
		if (srcPartialData == null) {
			return; // should not occur?
		}
		
		val prevDestData = dest.get(currPath);
		// ensure prevDestData != null;  ???
		
		// compare data fields + child
		val srcData = srcPartialData.dataIfPresent;
		if (srcData != null) {
			boolean eqData = compareDataFunc.test(srcData, prevDestData);
			if (eqData) {
				countEq++;
				visitor.visitNodeDataEq(currPath, srcPartialData, prevDestData, ctx);
			} else {
				countPutUpdate++;
				val destData = copyDataFunc.apply(srcData);
				dest.put(currPath, destData);
				visitor.visitNodeDataUpdate(currPath, srcPartialData, prevDestData, destData, ctx);
			}
		} else {
			// ignore compare
			countPartial++;
			visitor.visitNodeDataPartial(currPath, srcPartialData, prevDestData, ctx);
		}

		val srcChildNamesPresent = srcPartialData.childNamesPresent;
		if ((srcChildNamesPresent != null && !srcChildNamesPresent.isEmpty())
				|| srcPartialData.isCompleteChildNames) {
			val childListCtx = visitor.preVisitChildrenList(currPath, ctx);
		
			// step 1: scan srcChild, mark remainingDestChildNames if complete
			// recurse.. restricted only on present childNames
			val isComplete = srcPartialData.isCompleteChildNames;
			val remainDestChildNames = new LinkedHashSet<>(prevDestData.childNames); 

			if ((srcChildNamesPresent != null && !srcChildNamesPresent.isEmpty())) {
				for(val srcChildName: srcChildNamesPresent) {
					val childPath = currPath.toChild(srcChildName);
					
					boolean foundDestChild = remainDestChildNames.remove(srcChildName);
					if (foundDestChild) {
						recursiveMirrorCopy(childPath, childListCtx);
					} else {
						val childSrcDataPartial = src.getPartial(childPath);
						recursiveAdd(childPath, childSrcDataPartial, childListCtx);
					}
				}
			}
			
			// step 2: scan remainingDestChildNames
			if (isComplete) {
				if (! remainDestChildNames.isEmpty()) {
					for(val childName: remainDestChildNames) {
						val childPath = currPath.toChild(childName);
						val childDestData = dest.get(childPath);
						recursiveRemove(childPath, childDestData, childListCtx);
					}
				} // else ok, complete and all child handled => no remove
			} // else ignore remain child
			
			visitor.postVisitChildrenList(currPath, childListCtx);
		}
	}

	protected void recursiveAdd(NodeNamesPath currPath, PartialNodeData srcDataPartial, TCtx ctx) {
		if (srcDataPartial.dataIfPresent != null) {
			countPutAdd++;
			val srcData = srcDataPartial.dataIfPresent;
			val destData = copyDataFunc.apply(srcData);
			dest.put(currPath, destData);
		} else {
			// can not add?
		}
		visitor.visitNodeDataAdd(currPath, srcDataPartial, ctx);
		
		// recurse.. restricted only on present childNames
		val srcChildNamesPresent = srcDataPartial.childNamesPresent;
		if ((srcChildNamesPresent != null && !srcChildNamesPresent.isEmpty())) {
			val childListCtx = visitor.preVisitChildrenList(currPath, ctx);
			
			for(val srcChildName: srcChildNamesPresent) {
				val childPath = currPath.toChild(srcChildName);
				val childSrcDataPartial = src.getPartial(childPath);
				recursiveAdd(childPath, childSrcDataPartial, childListCtx);
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
