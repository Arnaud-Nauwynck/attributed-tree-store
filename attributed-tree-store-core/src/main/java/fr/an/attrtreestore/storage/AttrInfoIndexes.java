package fr.an.attrtreestore.storage;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.attrinfo.AttrInfo;
import lombok.val;

/**
 * mapping AttrInfo <-> int, to encode AttrInfo using 'int32 index' bits instead of 'String name'
 */
public class AttrInfoIndexes {

	private final ImmutableMap<String,Integer> attr2Index;
	private final ImmutableList<AttrInfo<Object>> index2Attr;
	
	// ------------------------------------------------------------------------
	
	public AttrInfoIndexes(Collection<AttrInfo<Object>> attrInfos) {
		this.index2Attr = ImmutableList.copyOf(attrInfos);
		val attr2IndexB = ImmutableMap.<String,Integer>builder();
		for(int i = 0; i < index2Attr.size(); i++) {
			attr2IndexB.put(index2Attr.get(i).name, i);
		}
		this.attr2Index = attr2IndexB.build();
	}
	
	// ------------------------------------------------------------------------

	public int attrToIndex(AttrInfo<?> attr) {
		return attr2Index.get(attr.name);
	}

	public AttrInfo<Object> indexToAttr(int index) {
		return indexToAttr(index);
	}

	public ImmutableList<AttrInfo<Object>> getIndex2Attr() {
		return index2Attr;
	}
	
}
