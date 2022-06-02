package fr.an.attrtreestore.api.override;

import fr.an.attrtreestore.api.NodeData;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class OverrideNodeData {

	public static final OverrideNodeData NOT_OVERRIDEN = new OverrideNodeData(OverrideNodeStatus.NOT_OVERRIDEN, null);
	public static final OverrideNodeData DELETED = new OverrideNodeData(OverrideNodeStatus.DELETED, null);
	
	public final OverrideNodeStatus status;
	public final NodeData data;
	
	@Override
	public String toString() {
		return "OverrideNodeData[" + status + "]";
	}

}