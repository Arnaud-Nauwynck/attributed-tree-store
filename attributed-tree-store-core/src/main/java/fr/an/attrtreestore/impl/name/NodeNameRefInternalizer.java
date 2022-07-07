package fr.an.attrtreestore.impl.name;

import org.path4j.NodeName;
import org.path4j.NodeNamesPath;

import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.NodeTreeDataVisitor.SimpleNodeTreeDataVisitor;
import fr.an.attrtreestore.impl.traversal.DefaultTreeDataWalker;
import lombok.val;

public class NodeNameRefInternalizer {

	private final NodeNameRefCounters refCounter;
	
	// ------------------------------------------------------------------------
	
	public NodeNameRefInternalizer(NodeNameRefCounters refCounter) {
		this.refCounter = refCounter;
	}

	// ------------------------------------------------------------------------
	
	public void recursiveIncrCount(TreeData tree) {
		val visitor = new SimpleNodeTreeDataVisitor() {
			@Override
			public void visitNodeData(NodeNamesPath path, NodeData data) {
				val foundCounter = refCounter.getCounter(data.name);
				if (foundCounter != null) {
					if (foundCounter.ref != data.name) {
						data._setNameInternalizedRef(foundCounter.ref);
					}
				}
				
				if (data.childNames != null) {
					val childNameRefs = ImmutableSet.<NodeName>builder(); // sorted.. same order as copied source
					int childNameRefsChanged = 0;
					for(val childName: data.childNames) {
						NodeName childNameRef = childName;
						val foundChildCounter = refCounter.getCounter(childName);
						if (foundChildCounter != null) {
							if (foundChildCounter.ref != data.name) {
								childNameRefsChanged++;
								childNameRef = foundChildCounter.ref;
							}
						}
						childNameRefs.add(childNameRef);
						
						// recurse.. cf walker
					}
					if (childNameRefsChanged != 0) {
						data._setChildNames_InternalizedRefs(childNameRefs.build());
					}
				}
			}
		};
		val walker = new DefaultTreeDataWalker<Void>(tree, visitor);
		walker.visitRecursiveRoot();
	}

}
