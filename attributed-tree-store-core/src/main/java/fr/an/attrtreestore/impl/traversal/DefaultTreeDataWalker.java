package fr.an.attrtreestore.impl.traversal;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataVisitor;
import fr.an.attrtreestore.api.traversal.TreeDataWalker;
import lombok.val;

/**
 * default implementation of TreeDataComparer
 */
public class DefaultTreeDataWalker<TCtx> extends TreeDataWalker<TCtx> {
	
	// ------------------------------------------------------------------------
	
	public DefaultTreeDataWalker(TreeData src,
			NodeTreeDataVisitor<TCtx> visitor
			) {
		super(src, visitor);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void visitRecursive(NodeNamesPath path, TCtx ctx) {
		val srcData = src.get(path);
		visitor.visitNodeData(path, srcData, ctx);

		val srcChildNames = srcData.childNames;
		if ((srcChildNames != null && !srcChildNames.isEmpty())) {
			val childListCtx = visitor.preVisitChildrenList(path, ctx);
			
			for(val srcChildName: srcChildNames) {
				val childPath = path.toChild(srcChildName);
				visitRecursive(childPath, childListCtx);
			}
			
			visitor.postVisitChildrenList(path, childListCtx);
		}
	}
	
}
