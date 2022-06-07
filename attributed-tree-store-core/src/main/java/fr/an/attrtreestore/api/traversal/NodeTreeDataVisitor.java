package fr.an.attrtreestore.api.traversal;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

public abstract class NodeTreeDataVisitor<TCtx> {

	public abstract TCtx rootCtx();
	
	public abstract void visitNodeData(NodeNamesPath path, NodeData data, TCtx ctx);

	public abstract TCtx preVisitChildrenList(NodeNamesPath path, TCtx ctx);
	
	public abstract void postVisitChildrenList(NodeNamesPath path, TCtx ctx);

	
	// ------------------------------------------------------------------------
	
	public static class DefaultNodeTreeDataVisitor<TCtx> extends NodeTreeDataVisitor<TCtx> {
		@Override
		public TCtx rootCtx() {
			return null;
		}
		
		@Override
		public void visitNodeData(NodeNamesPath path, NodeData data, TCtx ctx) {
			// do nothing
		}

		@Override
		public TCtx preVisitChildrenList(NodeNamesPath path, TCtx ctx) {
			// do nothing
			return null;
		}
		
		@Override
		public void postVisitChildrenList(NodeNamesPath path, TCtx ctx) {
			// do nothing
		}
	}
	
	public static abstract class SimpleNodeTreeDataVisitor extends DefaultNodeTreeDataVisitor<Void> {
		@Override
		public final void visitNodeData(NodeNamesPath path, NodeData data, Void ctx) {
			visitNodeData(path, data);
		}

		public abstract void visitNodeData(NodeNamesPath path, NodeData data);

	}


}
