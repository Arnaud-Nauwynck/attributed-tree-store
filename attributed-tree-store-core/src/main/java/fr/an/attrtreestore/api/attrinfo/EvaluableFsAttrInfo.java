package fr.an.attrtreestore.api.attrinfo;

public class EvaluableFsAttrInfo<T> extends AttrInfo<T> {

	// ------------------------------------------------------------------------
	
	public EvaluableFsAttrInfo(String name, Class<T> dataClass, AttrDataEncoder<T> attrDataEncoder) {
		super(name, dataClass, attrDataEncoder);
	}

	// ------------------------------------------------------------------------

	@Override
	public void accept(AttrInfoVisitor visitor) {
		visitor.caseEvaluableFsAttr(this);
	}

}
