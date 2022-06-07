package fr.an.attrtreestore.impl.name;

import java.util.HashMap;
import java.util.Map;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataVisitor.SimpleNodeTreeDataVisitor;
import fr.an.attrtreestore.impl.traversal.DefaultTreeDataWalker;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

public class NodeNameRefCounters {

	@RequiredArgsConstructor
	@Getter
	public static class NameRefCounter {
		final NodeName ref;
		// List<NodeName> otherRefs;
		int countSameRef;
		int countOtherRef;
	}
	
	private Map<NodeName,NameRefCounter> counters = new HashMap<>();
	
	public void incrCount(NodeName name) {
		NameRefCounter counter = counters.get(name);
		if (counter == null) {
			counter = new NameRefCounter(name);
			counters.put(name, counter);
		}
		if (counter.ref == name) {
			counter.countSameRef++;
		} else {
			counter.countOtherRef++;
		}
	}

	public void recursiveIncrCount(TreeData tree) {
		val visitor = new SimpleNodeTreeDataVisitor() {
			@Override
			public void visitNodeData(NodeNamesPath path, NodeData data) {
				incrCount(data.name);
				
				if (data.childNames != null) {
					for(val childName: data.childNames) {
						incrCount(childName);
						// recurse.. cf walker
					}
				}
			}
		};
		val walker = new DefaultTreeDataWalker<Void>(tree, visitor);
		walker.visitRecursiveRoot();
	}

	public NameRefCounter getCounter(NodeName name) {
		return counters.get(name);
	}

}
