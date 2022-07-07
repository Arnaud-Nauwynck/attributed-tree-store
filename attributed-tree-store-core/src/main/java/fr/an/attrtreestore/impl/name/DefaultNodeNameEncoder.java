package fr.an.attrtreestore.impl.name;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.api.name.NodeNameEncoderOption;
import fr.an.attrtreestore.api.name.NodeNameEncoderOption.EncodeSizeResult;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoderOptions.CharArrayNodeName;
import lombok.val;

public class DefaultNodeNameEncoder extends NodeNameEncoder {

	private final List<NodeNameEncoderOption> encoders;

	private int internalizeMaxPathLevel;
	
	private final Predicate<NodeName> decideInternalize;

	private Map<String,NodeName> interned;
	
	// ------------------------------------------------------------------------
	
	public DefaultNodeNameEncoder(Collection<NodeNameEncoderOption> encoders, 
			int internalizeMaxPathLevel,
			Predicate<NodeName> decideInternalize,
			Map<String,NodeName> interned) {
		this.encoders = new ArrayList<>(encoders);
		this.internalizeMaxPathLevel = internalizeMaxPathLevel;
		this.decideInternalize = decideInternalize;
		this.interned = (interned != null)? new HashMap<>(interned) : new HashMap<>();
	}

	public static DefaultNodeNameEncoder createDefault() {
		val encoders = new ArrayList<NodeNameEncoderOption>();
		encoders.addAll(ByteStructNodeNameEncoderOptions.DEFAULTS);
		encoders.addAll(CharStructNodeNameEncoderOptions.DEFAULTS);
		return new DefaultNodeNameEncoder(encoders, 3, null, new HashMap<>());
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public NodeName encode(String name) {
		val found = interned.get(name);
		if (found != null) {
			return found;
		}
		
		NodeName res = doEncode(name);
		
		if (decideInternalize != null) {
			boolean intern = decideInternalize.test(res);
			if (intern) {
				doPutInternalize(name, res);
			}
		}
		return res;
	}

	private void doPutInternalize(String name, NodeName res) {
		// do copy? (avoid lock / concurrent issues)
		val newInterned = new HashMap<>(interned);
		newInterned.put(name, res);
		this.interned = newInterned;
	}

	private NodeName doEncode(String name) {
		val stringEncodeSize = DefaultNodeNameEncoderOptions.defaultStringEncoderSize(name);
		NodeName res = null;
		int bestSizeSoFar = stringEncodeSize;
		EncodeSizeResult sizeRes = new EncodeSizeResult();
		sizeRes.estimatedSize = bestSizeSoFar;
		for(val e: encoders) {
			NodeName tmpres = e.tryEncode(name, bestSizeSoFar, sizeRes);
			if (tmpres != null && sizeRes.estimatedSize < bestSizeSoFar) {
				bestSizeSoFar = sizeRes.estimatedSize;
				res = tmpres;
			}
		}
		
		if (res == null) {
			// fallback, encode using default CharArrayNodeName
			res = new CharArrayNodeName(name.toCharArray());
		}
		return res;
	}

	/**
	 * override to force pathElts NodeName with level < internalizeMaxPathLevel to be internalized
	 */
	@Override
	public NodeName[] encodePathNodeNames(String[] pathElts) {
		val pathCount = pathElts.length;
		val res = new NodeName[pathCount];
		for(int i = 0; i < pathCount; i++) {
			val pathElt = pathElts[i];
			NodeName resElt;
//			if (i < internalizeMaxPathLevel) {
//				resElt = interned.get(pathElt);
//				if (resElt == null) {
//					resElt = doEncode(pathElt);
//					doPutInternalize(pathElt, resElt);
//				}
//			} else {
				resElt = encode(pathElt);
//			}
			res[i] = resElt;
		}
		return res;
	}
	
}
