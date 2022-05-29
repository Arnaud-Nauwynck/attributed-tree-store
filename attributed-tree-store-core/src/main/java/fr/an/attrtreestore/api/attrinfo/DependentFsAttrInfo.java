package fr.an.attrtreestore.api.attrinfo;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.val;

public class DependentFsAttrInfo<T> extends AttrInfo<T> {

	@Getter
	private final ImmutableList<AttrInfo<Object>> nodeAttrDependencies;
	
	// ------------------------------------------------------------------------
	
	public DependentFsAttrInfo(String name, Class<T> dataClass, AttrDataEncoder<T> attrDataEncoder, //
			ImmutableList<AttrInfo<Object>> nodeAttrDependencies) {
		super(name, dataClass, attrDataEncoder);
		this.nodeAttrDependencies = nodeAttrDependencies;
		// register inverse dependencies
		for(val attr : nodeAttrDependencies) {
			attr._inv_addPropagateToNodeAttr(this);
		}
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void accept(AttrInfoVisitor visitor) {
		visitor.caseDependentAttr(this);
	}

}
