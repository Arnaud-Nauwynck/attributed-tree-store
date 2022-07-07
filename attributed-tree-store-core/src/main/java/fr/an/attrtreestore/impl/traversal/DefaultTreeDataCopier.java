package fr.an.attrtreestore.impl.traversal;

import java.util.function.Function;

import org.path4j.NodeNamesPath;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.traversal.TreeDataCopier;
import lombok.val;

/**
 * default implementation of TreeDataCopier
 */
public class DefaultTreeDataCopier extends TreeDataCopier {
		
	// ------------------------------------------------------------------------
	
	public DefaultTreeDataCopier(TreeData src, IWriteTreeData dest, //
			Function<NodeData, NodeData> copyDataFunc
			) {
		super(src, dest, copyDataFunc);
	}

	public DefaultTreeDataCopier(TreeData src, IWriteTreeData dest) {
		super(src, dest, IDENTITY_COPY_FUNC);
	}

	public static void copy(TreeData src, IWriteTreeData dest) {
		val copier = new DefaultTreeDataCopier(src, dest);
		copier.executeCopyTreeData();
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public void executeCopyTreeData() {
		val rootPath = NodeNamesPath.ROOT;
		
		recursiveCopy(rootPath);
	}
	
	public void recursiveCopy(NodeNamesPath currPath) {
		val srcData = src.get(currPath);
		if (srcData == null) {
		    return;
		}

		val destData = copyDataFunc.apply(srcData);
		dest.put(currPath, destData);

		val srcChildNames = srcData.childNames;
		if ((srcChildNames != null && !srcChildNames.isEmpty())) {
			for(val srcChildName: srcChildNames) {
				val childPath = currPath.toChild(srcChildName);
				
				recursiveCopy(childPath);
			}
		}
	}

}
