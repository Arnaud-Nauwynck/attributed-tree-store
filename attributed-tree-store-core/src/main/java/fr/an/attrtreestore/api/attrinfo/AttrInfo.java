package fr.an.attrtreestore.api.attrinfo;

import java.util.ArrayList;
import java.util.List;

public abstract class AttrInfo<T> {

	public final String name;
	public final Class<T> dataClass;

	public final AttrDataEncoder<T> attrDataEncoder;
	
	private List<AttrInfo<Object>> inv_propagateToParentAttrs = new ArrayList<>();
	private List<AttrInfo<Object>> inv_propagateToChildAttrs = new ArrayList<>();
	private List<AttrInfo<Object>> inv_propagateToNodeAttrs = new ArrayList<>();
	
	// ------------------------------------------------------------------------
	
	public AttrInfo(String name, Class<T> dataClass, AttrDataEncoder<T> attrDataEncoder) {
		this.name = name;
		this.dataClass = dataClass;
		this.attrDataEncoder = attrDataEncoder;
	}
	
	// ------------------------------------------------------------------------
	
	public abstract void accept(AttrInfoVisitor visitor);

	/*pp*/ void _inv_addPropagateToParentAttr(AttrInfo<?> attr) {
		@SuppressWarnings("unchecked")
		AttrInfo<Object> attrObj = (AttrInfo<Object>) attr;
		inv_propagateToParentAttrs.add(attrObj);
	}

	/*pp*/ void _inv_addPropagateToChildAttr(AttrInfo<?> attr) {
		@SuppressWarnings("unchecked")
		AttrInfo<Object> attrObj = (AttrInfo<Object>) attr;
		inv_propagateToChildAttrs.add(attrObj);
	}

	/*pp*/ void _inv_addPropagateToNodeAttr(AttrInfo<?> attr) {
		@SuppressWarnings("unchecked")
		AttrInfo<Object> attrObj = (AttrInfo<Object>) attr;
		inv_propagateToNodeAttrs.add(attrObj);
	}

}
