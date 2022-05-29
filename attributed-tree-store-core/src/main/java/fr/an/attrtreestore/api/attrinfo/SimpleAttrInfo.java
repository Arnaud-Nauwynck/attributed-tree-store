package fr.an.attrtreestore.api.attrinfo;

public class SimpleAttrInfo<T> extends AttrInfo<T> {

	// ------------------------------------------------------------------------
	
	public SimpleAttrInfo(String name, Class<T> dataClass, AttrDataEncoder<T> attrDataEncoder) {
		super(name, dataClass, attrDataEncoder);
	}

	// ------------------------------------------------------------------------

	@Override
	public void accept(AttrInfoVisitor visitor) {
		visitor.caseSimpleAttr(this);
	}

}
