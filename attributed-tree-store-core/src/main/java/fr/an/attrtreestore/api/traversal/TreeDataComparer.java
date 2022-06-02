package fr.an.attrtreestore.api.traversal;

import java.util.function.BiPredicate;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import lombok.RequiredArgsConstructor;

/**
 * action to compare 'TreeData' from a source 'TreeData'
 */
@RequiredArgsConstructor
public abstract class TreeDataComparer<TCtx> {

	protected final TreeData src;
	protected final TreeData dest;
	
	protected final BiPredicate<NodeData,NodeData> compareDataFunc;
	
	protected final NodeTreeDataDiffVisitor<TCtx> visitor;
	
	protected int countEq;
	protected int countPutAdd;
	protected int countPutUpdate;
	protected int countRemove;

	// ------------------------------------------------------------------------

	public abstract void executeCompare();
	
}
