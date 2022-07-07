package fr.an.attrtreestore.api.partial;

import org.path4j.NodeNamesPath;

/**
 * cf sub-classes implementations:
 * - InMem_PartialTreeData
 *
 */
public abstract class PartialTreeData {

	public abstract PartialNodeData getPartial(NodeNamesPath path);

}
