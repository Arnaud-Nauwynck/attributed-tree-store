package fr.an.attrtreestore.api.traversal;

import java.util.function.BiPredicate;
import java.util.function.Function;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.partial.PartialTreeData;
import lombok.RequiredArgsConstructor;

/**
 * action to synchronize partially of mirror copy (writable) 'TreeData' from a partial source (readonly) 'PartialTreeData'
 */
@RequiredArgsConstructor
public abstract class PartialTreeDataMirrorCopier<TCtx> {

	protected final PartialTreeData src;
	protected final IWriteTreeData dest;
	
	protected final BiPredicate<NodeData,NodeData> compareDataFunc;
	protected final Function<NodeData,NodeData> copyDataFunc;
	
	protected final NodeTreeDataPartialDiffVisitor<TCtx> visitor;

	protected int countPartial;
	protected int countEq;
	protected int countPutAdd;
	protected int countPutUpdate;
	protected int countRemove;

	// ------------------------------------------------------------------------

	public abstract void executeMirrorCopyTreeData();
	
}
