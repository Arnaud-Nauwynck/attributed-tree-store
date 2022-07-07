package fr.an.attrtreestore.impl.traversal;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataVisitor;
import lombok.Getter;
import lombok.val;

public class CounterNodeTreeDataVisitor extends NodeTreeDataVisitor<Void> {

	@Getter
	private TreeCount counter = new TreeCount();
	
	public static class TreeCount {
		
		@Getter
		private int count;
		
		@Getter
		private int type1Count;
		
		@Getter
		private int type2Count;
		
		@Getter
		private int childListCount;
		
	}
	
	@Override
	public Void rootCtx() {
		return null;
	}
	
	@Override
	public void visitNodeData(NodeNamesPath path, NodeData data, Void ctx) {
		counter.count++;
		val type = (data != null)? data.type : 0;
		if (type == 1) {
			counter.type1Count++;
		} else if (type == 2) {
			counter.type2Count++;
		}
	}

	@Override
	public Void preVisitChildrenList(NodeNamesPath path, Void ctx) {
		counter.childListCount++;
		return null;
	}
	
	@Override
	public void postVisitChildrenList(NodeNamesPath path, Void ctx) {
	}

}
