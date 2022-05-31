package fr.an.attrtreestore.storage.api;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;

/**
 * abstract class equivalent to <code> Map<NodeNamesPath, NodeData> </code>
 *
 */
public abstract class PartialNodeDataByPath {

	public abstract void put(NodeNamesPath path, NodeData data);
	
	public abstract void remove(NodeNamesPath path);
	
	public abstract NodeOverrideData get(NodeNamesPath path);
	
}
