package fr.an.attrtreestore.api.attrinfo;

public abstract class AttrInfoVisitor {

	public abstract <T> void caseSimpleAttr(SimpleAttrInfo<T> attr);

	public abstract <T> void caseEvaluableFsAttr(EvaluableFsAttrInfo<T> attr);

	public abstract <T> void caseDependentAttr(DependentFsAttrInfo<T> attr);
	
	public abstract <T> void caseInheritedAttr(InheritedAttrInfo<T> attr);
	
	public abstract <T> void caseSynthetizedAttr(SynthethizedAttrInfo<T> attr);

}
