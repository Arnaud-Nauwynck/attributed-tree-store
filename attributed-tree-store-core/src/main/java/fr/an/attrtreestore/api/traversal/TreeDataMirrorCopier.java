package fr.an.attrtreestore.api.traversal;

import java.util.function.BiPredicate;
import java.util.function.Function;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import lombok.RequiredArgsConstructor;

/**
 * action to synchronize a mirror copy (writable) 'TreeData' from a source (readonly) 'TreeData'
 */
@RequiredArgsConstructor
public abstract class TreeDataMirrorCopier<TCtx> {

	protected final TreeData src;
	protected final IWriteTreeData dest;
	
	protected final BiPredicate<NodeData,NodeData> compareDataFunc;
	protected final Function<NodeData,NodeData> copyDataFunc;
	
	protected final NodeTreeDataDiffVisitor<TCtx> visitor;

	protected int countEq;
	protected int countPutAdd;
	protected int countPutUpdate;
	protected int countRemove;

	// ------------------------------------------------------------------------

	public abstract void executeMirrorCopyTreeData();
	
}
