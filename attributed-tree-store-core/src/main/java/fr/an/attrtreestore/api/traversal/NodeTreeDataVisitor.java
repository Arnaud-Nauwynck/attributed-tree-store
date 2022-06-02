package fr.an.attrtreestore.api.traversal;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

public abstract class NodeTreeDataVisitor<TCtx> {

	public abstract TCtx rootCtx();
	
	public abstract void visitNodeData(NodeNamesPath path, NodeData data, TCtx ctx);

	public abstract TCtx preVisitChildrenList(NodeNamesPath path, TCtx ctx);
	
	public abstract void postVisitChildrenList(NodeNamesPath path, TCtx ctx);

}
