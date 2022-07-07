package fr.an.attrtreestore.storage.old;

import org.path4j.NodeName;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeNameAndFilePos {
	public final NodeName name;
	public final long filePos; 
}
