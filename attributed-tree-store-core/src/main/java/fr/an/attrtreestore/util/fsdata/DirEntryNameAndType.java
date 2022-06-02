package fr.an.attrtreestore.util.fsdata;

import fr.an.attrtreestore.api.NodeName;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DirEntryNameAndType {
	public final NodeName name;
	public final FsNodeType type;
}