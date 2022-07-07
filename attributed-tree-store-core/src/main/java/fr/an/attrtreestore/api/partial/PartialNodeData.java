package fr.an.attrtreestore.api.partial;

import java.util.Set;

import org.path4j.NodeName;

import fr.an.attrtreestore.api.NodeData;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PartialNodeData {
	
	public final NodeData dataIfPresent;

	public final boolean isCompleteChildNames;
	public final Set<NodeName> childNamesPresent;
	public final Set<NodeName> childNamesKnownRemoved;
	
	@Override
	public String toString() {
		return "PartialNodeStatus[" 
				+ ((dataIfPresent == null)? "!data" : "")
				+ ((!isCompleteChildNames)? " !completeChildNames" : "")
				+ ((!isCompleteChildNames && childNamesPresent != null)? " childNamesPresent:[" + childNamesPresent + "]": "")
				+ ((!isCompleteChildNames && childNamesKnownRemoved != null)? " childNamesKnownRemoved:[" + childNamesKnownRemoved + "]" : "")
				+ "]";
	}

}
