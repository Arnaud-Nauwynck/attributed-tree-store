package fr.an.attrtreestore.impl.name;

import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_ArrayHeaderSize;
import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_CharSize;
import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_IntSize;
import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_ObjectHeaderSize;
import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_RefSize;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.name.NodeNameEncoderOption;
import lombok.AllArgsConstructor;

public class DefaultNodeNameEncoderOptions {

	public static final NodeName EMPTY_NAME = new StringNodeName("");

	public static final StringNodeNameEncoderOption CHAR_ARRAY_OPTION = new StringNodeNameEncoderOption();
	public static final StringNodeNameEncoderOption STRING_OPTION = new StringNodeNameEncoderOption();

	public static int defaultStringEncoderSize(String name) {
		return VM_ObjectHeaderSize // for StringNodeName object
				+ VM_RefSize // for StringNodeName.name
				+ VM_ObjectHeaderSize // for java.lang.String object
				+ VM_RefSize // for java.lang.String.value ref
				+ VM_IntSize // for java.lan.String.hash
				+ VM_ArrayHeaderSize // for char[] array object
				+ name.length() * VM_CharSize; // for chars element (maybe ascii compressed by jvm?)
	}
	
	public static int defaultCharArrayEncoderSize(char[] name) {
		return VM_ObjectHeaderSize // for CharArrayNodeName object
				+ VM_RefSize // for StringNodeName.name
				+ VM_ArrayHeaderSize // for char[] array object
				+ name.length * VM_CharSize; // for chars element (maybe ascii compressed by jvm?)
	}
	
	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class StringNodeName extends NodeName {

		public final String name;
		
		@Override
		public String toText() {
			return name;
		}
	}
	
	public static class StringNodeNameEncoderOption extends NodeNameEncoderOption {
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			sizeResult.estimatedSize = defaultStringEncoderSize(name);
			return new StringNodeName(name);
		}
	}

	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class CharArrayNodeName extends NodeName {

		private final char[] name;
		
		@Override
		public String toText() {
			return new String(name);
		}
	}
	
	public static class CharArrayNodeNameEncoderOption extends NodeNameEncoderOption {
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			char[] charArray = name.toCharArray();
			sizeResult.estimatedSize = defaultCharArrayEncoderSize(charArray);
			return new CharArrayNodeName(charArray);
		}
	}

}
