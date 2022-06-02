package fr.an.attrtreestore.storage.old;

import fr.an.attrtreestore.api.NodeName;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeNameAndFilePos {
	public final NodeName name;
	public final long filePos; 
}
