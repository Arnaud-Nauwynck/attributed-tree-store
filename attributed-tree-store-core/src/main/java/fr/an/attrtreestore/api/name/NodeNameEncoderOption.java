package fr.an.attrtreestore.api.name;

import fr.an.attrtreestore.api.NodeName;

public abstract class NodeNameEncoderOption {

	public static class EncodeSizeResult {
		public int estimatedSize;
	}
	
	public abstract NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult);

}
