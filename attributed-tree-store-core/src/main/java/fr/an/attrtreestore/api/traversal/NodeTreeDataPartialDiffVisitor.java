package fr.an.attrtreestore.api.traversal;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.partial.PartialNodeData;

public abstract class NodeTreeDataPartialDiffVisitor<TCtx> {

	public abstract TCtx rootCtx();
	
	public abstract void visitNodeDataPartial(NodeNamesPath path, PartialNodeData srcData, NodeData destData, TCtx ctx);
	public abstract void visitNodeDataEq(NodeNamesPath path, PartialNodeData srcData, NodeData destData, TCtx ctx);
	public abstract void visitNodeDataAdd(NodeNamesPath path, PartialNodeData srcData, TCtx ctx);
	public abstract void visitNodeDataUpdate(NodeNamesPath path, PartialNodeData srcData, NodeData prevDestData, NodeData destData, TCtx ctx);
	public abstract void visitNodeDataRemove(NodeNamesPath path, NodeData destData, TCtx ctx);

	public abstract TCtx preVisitChildrenList(NodeNamesPath path, TCtx ctx);
	
	public abstract void postVisitChildrenList(NodeNamesPath path, TCtx ctx);

	
	// ------------------------------------------------------------------------
	
	public static final NodeTreeDataPartialDiffVisitor<Void> NOOP_VISITOR = new DefaultNodeTreeDataPartialDiffVisitor<Void>();
	
	public static class DefaultNodeTreeDataPartialDiffVisitor<TCtx> extends NodeTreeDataPartialDiffVisitor<TCtx> {

		@Override
		public TCtx rootCtx() {
			return null;
		}

		@Override
		public void visitNodeDataPartial(NodeNamesPath path, PartialNodeData srcData, NodeData destData, TCtx ctx) {
		}

		@Override
		public void visitNodeDataEq(NodeNamesPath path, PartialNodeData srcData, NodeData destData, TCtx ctx) {
		}

		@Override
		public void visitNodeDataAdd(NodeNamesPath path, PartialNodeData srcData, TCtx ctx) {
		}

		@Override
		public void visitNodeDataUpdate(NodeNamesPath path, PartialNodeData srcData, NodeData prevDestData, NodeData destData, TCtx ctx) {
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
