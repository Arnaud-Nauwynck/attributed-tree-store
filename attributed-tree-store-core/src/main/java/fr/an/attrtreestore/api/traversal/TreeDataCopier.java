package fr.an.attrtreestore.api.traversal;

import java.util.function.Function;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import lombok.RequiredArgsConstructor;

/**
 * action to copy (writable) 'TreeData' from a source (readonly) 'TreeData'
 */
@RequiredArgsConstructor
public abstract class TreeDataCopier {

	public static final Function<NodeData, NodeData> IDENTITY_COPY_FUNC = (data) -> data;

	protected final TreeData src;
	protected final IWriteTreeData dest;
	
	protected final Function<NodeData, NodeData> copyDataFunc;
	
	// ------------------------------------------------------------------------

	public abstract void executeCopyTreeData();
	
}
