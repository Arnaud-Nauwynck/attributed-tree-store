package fr.an.attrtreestore.storage.impl;

import java.util.List;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.override.OverrideNodeData;
import fr.an.attrtreestore.api.override.OverrideTreeData;
import lombok.Getter;
import lombok.val;

/**
 * compound OverrideTreeData, delegating to sequence of OverrideTreeData
 */
public class Compound_OverrideTreeData extends OverrideTreeData {

	@Getter
	protected OverrideTreeData[] sequenceOverrideTrees;
	protected OverrideTreeData currWriteLast; // redundant with sequenceOverrideTrees[sequenceOverrideTrees.length - 1];

	// ------------------------------------------------------------------------
	
	public Compound_OverrideTreeData(List<? extends OverrideTreeData> src) {
		this(src.toArray(new Compound_OverrideTreeData[src.size()]));
	}

	public Compound_OverrideTreeData(OverrideTreeData[] src) {
		this.sequenceOverrideTrees = src;
		this.currWriteLast = src[src.length - 1];
	}

	
	// implements api OverrideTreeData
	// ------------------------------------------------------------------------

	@Override
	public OverrideNodeData getOverride(NodeNamesPath path) {
		OverrideNodeData res = currWriteLast.getOverride(path);
		if (res != null) {
			return res;
		}
		for(int i = sequenceOverrideTrees.length-2; i >= 0; i--) {
			res = sequenceOverrideTrees[i].getOverride(path);
			if (res != null) {
				return res;
			}
		}
		return null;
	}

	@Override
	public void put(NodeNamesPath path, NodeData data) {
		currWriteLast.put(path, data);
	}

	@Override
	public void remove(NodeNamesPath path) {
		currWriteLast.remove(path);
	}

	// ------------------------------------------------------------------------
	
	public synchronized OverrideTreeData[] rollAddWal(OverrideTreeData newWalOverrideTree) {
		val prevArray = sequenceOverrideTrees;
		val len = prevArray.length;
		val newArray = new OverrideTreeData[len + 1];
		System.arraycopy(prevArray, len, newArray, 0, len);
		newArray[len] = newWalOverrideTree;
		this.sequenceOverrideTrees = newArray;
		this.currWriteLast = newWalOverrideTree;
		return prevArray;
	}

	// ------------------------------------------------------------------------
	
	
	
}
