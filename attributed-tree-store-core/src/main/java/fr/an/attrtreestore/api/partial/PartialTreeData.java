package fr.an.attrtreestore.api.partial;

import fr.an.attrtreestore.api.NodeNamesPath;

/**
 * cf sub-classes implementations:
 * - InMem_PartialTreeData
 *
 */
public abstract class PartialTreeData {

	public abstract PartialNodeData getPartial(NodeNamesPath path);

}
