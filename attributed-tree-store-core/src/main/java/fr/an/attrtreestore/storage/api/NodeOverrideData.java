package fr.an.attrtreestore.storage.api;

import fr.an.attrtreestore.api.NodeData;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeOverrideData {

	public static final NodeOverrideData NOT_OVERRIDEN = new NodeOverrideData(NodeOverrideStatus.NOT_OVERRIDEN, null);
	public static final NodeOverrideData DELETED = new NodeOverrideData(NodeOverrideStatus.DELETED, null);
	
	public final NodeOverrideStatus status;
	public final NodeData data;
	
	@Override
	public String toString() {
		return "NodeOverrideData[" + status + "]";
	}
	
}