package fr.an.attrtreestore.api.partial;

import java.util.Set;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
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
