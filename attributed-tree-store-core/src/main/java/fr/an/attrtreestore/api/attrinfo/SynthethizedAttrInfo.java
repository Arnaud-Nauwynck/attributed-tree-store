package fr.an.attrtreestore.api.attrinfo;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.val;

public class SynthethizedAttrInfo<T> extends AttrInfo<T> {

	@Getter
	private final ImmutableList<AttrInfo<Object>> childAttrDependencies;
	// private final ImmutableList<AttrInfo<Object>> nodeAttrDependencies;
	
	// ------------------------------------------------------------------------
	
	public SynthethizedAttrInfo(String name, Class<T> dataClass, AttrDataEncoder<T> attrDataEncoder, //
			ImmutableList<AttrInfo<Object>> childAttrDependencies) {
		super(name, dataClass, attrDataEncoder);
		this.childAttrDependencies = childAttrDependencies;
		// register inverse dependencies
		for(val attr : childAttrDependencies) {
			attr._inv_addPropagateToParentAttr(this);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public void accept(AttrInfoVisitor visitor) {
		visitor.caseSynthetizedAttr(this);
	}

}
