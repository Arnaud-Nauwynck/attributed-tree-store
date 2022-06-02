package fr.an.attrtreestore.api.traversal;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

public abstract class NodeTreeDataDiffVisitor<TCtx> {

	public abstract TCtx rootCtx();
	
	public abstract void visitNodeDataEq(NodeNamesPath path, NodeData srcData, NodeData destData, TCtx ctx);
	public abstract void visitNodeDataAdd(NodeNamesPath path, NodeData srcData, TCtx ctx);
	public abstract void visitNodeDataUpdate(NodeNamesPath path, NodeData srcData, NodeData prevDestData, NodeData destData, TCtx ctx);
	public abstract void visitNodeDataRemove(NodeNamesPath path, NodeData destData, TCtx ctx);

	public abstract TCtx preVisitChildrenList(NodeNamesPath path, TCtx ctx);
	
	public abstract void postVisitChildrenList(NodeNamesPath path, TCtx ctx);

	
	// ------------------------------------------------------------------------
	
	public static final NodeTreeDataDiffVisitor<Void> NOOP_VISITOR = new DefaultNodeTreeDataDiffVisitor<Void>();
	
	public static class DefaultNodeTreeDataDiffVisitor<TCtx> extends NodeTreeDataDiffVisitor<TCtx> {

		@Override
		public TCtx rootCtx() {
			return null;
		}

		@Override
		public void visitNodeDataEq(NodeNamesPath path, NodeData srcData, NodeData destData, TCtx ctx) {
		}

		@Override
		public void visitNodeDataAdd(NodeNamesPath path, NodeData srcData, TCtx ctx) {
		}

		@Override
		public void visitNodeDataUpdate(NodeNamesPath path, NodeData srcData, NodeData prevDestData, NodeData destData, TCtx ctx) {
		}

		@Override
		public void visitNodeDataRemove(NodeNamesPath path, NodeData destData, TCtx ctx) {
		}

		@Override
		public TCtx preVisitChildrenList(NodeNamesPath path, TCtx ctx) {
			return null;
		}

		@Override
		public void postVisitChildrenList(NodeNamesPath path, TCtx ctx) {
		}
		
	}
}
