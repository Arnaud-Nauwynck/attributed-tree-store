package fr.an.attrtreestore.api.attrinfo;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.val;

public class InheritedAttrInfo<T> extends AttrInfo<T> {

	@Getter
	private final ImmutableList<AttrInfo<Object>> parentAttrDependencies;
	// private final ImmutableList<AttrInfo<Object>> nodeAttrDependencies;

	// ------------------------------------------------------------------------
	
	public InheritedAttrInfo(String name, Class<T> dataClass, AttrDataEncoder<T> attrDataEncoder, //
			ImmutableList<AttrInfo<Object>> parentAttrDependencies) {
		super(name, dataClass, attrDataEncoder);
		this.parentAttrDependencies = parentAttrDependencies;
		// register inverse dependencies
		for(val attr : parentAttrDependencies) {
			attr._inv_addPropagateToChildAttr(this);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public void accept(AttrInfoVisitor visitor) {
		visitor.caseInheritedAttr(this);
	}

}
