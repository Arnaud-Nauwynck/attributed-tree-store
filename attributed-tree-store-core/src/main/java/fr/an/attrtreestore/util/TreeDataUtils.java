package fr.an.attrtreestore.util;

import java.util.List;
import java.util.Map;

import fr.an.attrtreestore.api.IReadTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.val;

public class TreeDataUtils {

	public static NodeData getWithChild(IReadTreeData tree,
			NodeNamesPath path,
			Map<NodeName,NodeData> foundChildMap,
			List<NodeName> notFoundChildLs) {
		NodeData res = tree.get(path);
		if (res == null) {
			return null;
		}
		val childNames = res.childNames;
		if (childNames != null && ! childNames.isEmpty()) {
			// may add .. foundChildLs.ensureCapacity(Math.min())
			for(val childName: childNames) {
				val childPath = path.toChild(childName); 
				val childData = tree.get(childPath);
				if (childData != null) {
					foundChildMap.put(childName, childData);
				} else {
					notFoundChildLs.add(childName);
				}
			}
		}		
		return res;
	}

}
