package fr.an.attrtreestore.api.traversal;

import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * action to walk in 'TreeData'
 */
@RequiredArgsConstructor
public abstract class TreeDataWalker<TCtx> {

	protected final TreeData src;

	protected final NodeTreeDataVisitor<TCtx> visitor;
	
	// ------------------------------------------------------------------------

	public void visitRecursiveRoot() {
		val rootCtx = visitor.rootCtx();
		
		visitRecursive(NodeNamesPath.ROOT, rootCtx);
	}

	public abstract void visitRecursive(NodeNamesPath path, TCtx ctx);

}
