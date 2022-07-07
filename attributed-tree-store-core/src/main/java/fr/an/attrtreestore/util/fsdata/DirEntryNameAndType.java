package fr.an.attrtreestore.util.fsdata;

import org.path4j.NodeName;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DirEntryNameAndType {
	public final NodeName name;
	public final FsNodeType type;
}