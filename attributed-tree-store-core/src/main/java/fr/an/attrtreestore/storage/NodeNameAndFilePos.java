package fr.an.attrtreestore.storage;

import fr.an.attrtreestore.api.NodeName;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeNameAndFilePos {
	public final NodeName name;
	public final long filePos; 
}
