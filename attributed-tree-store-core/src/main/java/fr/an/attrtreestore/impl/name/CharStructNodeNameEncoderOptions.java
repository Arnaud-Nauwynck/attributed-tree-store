package fr.an.attrtreestore.impl.name;

import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_CharSize;
import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_ObjectHeaderSize;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.collect.ImmutableList;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.name.NodeNameEncoderOption;
import lombok.AllArgsConstructor;

/**
 * compression strategy for using embedded char fields of small fixed size, 
 * instead of "String" containing "char[]"
 *
 * .. see also similar compression, using byte[] for ascii char in range 00-7F 
 */
public final class CharStructNodeNameEncoderOptions {

	public static final ImmutableList<NodeNameEncoderOption> DEFAULTS = ImmutableList.<NodeNameEncoderOption>of(
			// FALLBACK,
			new Char1NodeNameEncoderOption(),
			new Char2NodeNameEncoderOption(),
			new Char3NodeNameEncoderOption(),
			new Char4NodeNameEncoderOption(),
			new Char5NodeNameEncoderOption(),
			new Char6NodeNameEncoderOption(),
			new Char7NodeNameEncoderOption(),
			new Char8NodeNameEncoderOption(),
			new Char9NodeNameEncoderOption(),
			new Char10NodeNameEncoderOption(),
			new Char11NodeNameEncoderOption(),
			new Char12NodeNameEncoderOption(),
			new Char13NodeNameEncoderOption(),
			new Char14NodeNameEncoderOption(),
			new Char15NodeNameEncoderOption(),
			new Char16NodeNameEncoderOption()
			);
	

	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class Char1NodeName extends NodeName {
		public final char ch0;

		@Override
		public String toText() {
			return String.valueOf(ch0);
		}

		@Override
		public void appendTo(StringBuilder out) {
			out.append(ch0);
		}

		@Override
		public void appendTo(PrintStream out) {
			out.append(ch0);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen(ch0);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar(out, ch0);
		}

		@Override
		public int hashCode() {
			return ch0;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char1NodeName other = (Char1NodeName) obj;
				return ch0 == other.ch0;
			} else {
				return super.equals(obj);
			}
		}

	}
	
	public static class Char1NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ VM_CharSize // + VM_Padding6Size
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 1) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char1NodeName(name.charAt(0));
		}
	}

	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class Char2NodeName extends NodeName {
		public final char ch0;
		public final char ch1;

		@Override
		public String toText() {
			return "" + ch0 + ch1;
		}

		@Override
		public void appendTo(StringBuilder out) {
			out.append(ch0);
			out.append(ch1);
		}

		@Override
		public void appendTo(PrintStream out) {
			out.append(ch0);
			out.append(ch1);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen2(ch0, ch1);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar2(out, ch0, ch1);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return ch0 * prime + ch1;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char2NodeName other = (Char2NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char2NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 2 * VM_CharSize // + VM_Padding4Size
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 2) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char2NodeName(name.charAt(0), name.charAt(1));
		}
	}

	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class Char3NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2;
		}

		@Override
		public void appendTo(StringBuilder out) {
			out.append(ch0);
			out.append(ch1);
			out.append(ch2);
		}

		@Override
		public void appendTo(PrintStream out) {
			out.append(ch0);
			out.append(ch1);
			out.append(ch2);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen3(ch0, ch1, ch2);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar3(out, ch0, ch1, ch2);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return (ch0 * prime + ch1) * prime + ch2;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char3NodeName other = (Char3NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2;
			} else {
				return super.equals(obj);
			}
		}

//		@Override
//		public int compareTo(NodeName other) {
//			if (other instanceof )
//			return super.compareTo(other);
//		}
		
	}
	
	public static class Char3NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 3 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 3) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char3NodeName(name.charAt(0), name.charAt(1), name.charAt(2));
		}
	}

	// ------------------------------------------------------------------------

	private static void appendTo4(StringBuilder out, char ch0, char ch1, char ch2, char ch3) {
		out.append(ch0);
		out.append(ch1);
		out.append(ch2);
		out.append(ch3);
	}

	private static void appendTo4(PrintStream out, char ch0, char ch1, char ch2, char ch3) {
		out.append(ch0);
		out.append(ch1);
		out.append(ch2);
		out.append(ch3);
	}

	private static int hashCode4(char ch0, char ch1, char ch2, char ch3) {
		final int prime = 31;
		return ((ch0 * prime + ch1) * prime + ch2) * prime + ch3;
	}

	
	private static void appendTo8(StringBuilder out, char ch0, char ch1, char ch2, char ch3, char ch4, char ch5, char ch6, char ch7) {
		out.append(ch0);
		out.append(ch1);
		out.append(ch2);
		out.append(ch3);
		out.append(ch4);
		out.append(ch5);
		out.append(ch6);
		out.append(ch7);
	}

	private static void appendTo8(PrintStream out, char ch0, char ch1, char ch2, char ch3, char ch4, char ch5, char ch6, char ch7) {
		out.append(ch0);
		out.append(ch1);
		out.append(ch2);
		out.append(ch3);
		out.append(ch4);
		out.append(ch5);
		out.append(ch6);
		out.append(ch7);
	}

	private static int hashCode8(char ch0, char ch1, char ch2, char ch3, char ch4, char ch5, char ch6, char ch7) {
		final int prime = 31;
		return hashCode4(ch0, ch1, ch2, ch3) * prime + hashCode4(ch4, ch5, ch6, ch7);
	}

	private static int hashCode12(char ch0, char ch1, char ch2, char ch3, char ch4, char ch5, char ch6, char ch7,
			char ch8, char ch9, char ch10, char ch11) {
		final int prime = 31;
		return hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) * prime + hashCode4(ch8, ch9, ch10, ch11);
	}

	
	// ------------------------------------------------------------------------
	
	
	@AllArgsConstructor
	public static class Char4NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen4(ch0, ch1, ch2, ch3);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar4(out, ch0, ch1, ch2, ch3);
		}

		@Override
		public int hashCode() {
			return hashCode4(ch0, ch1, ch2, ch3);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char4NodeName p = (Char4NodeName) obj;
				return ch0 == p.ch0 && ch1 == p.ch1 && ch2 == p.ch2 && ch3 == p.ch3;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char4NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 4 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 4) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char4NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char5NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append(ch4);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append(ch4);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen4(ch0, ch1, ch2, ch3) + DataOutputUTFUtils.utfLen(ch4);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar4(out, ch0, ch1, ch2, ch3);
			DataOutputUTFUtils.writeUTFChar(out, ch4);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return hashCode4(ch0, ch1, ch2, ch3) * prime + ch4;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char5NodeName p = (Char5NodeName) obj;
				return ch0 == p.ch0 && ch1 == p.ch1 && ch2 == p.ch2 && ch3 == p.ch3
						&& ch4 == p.ch4;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char5NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 5 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 5) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char5NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), name.charAt(4));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char6NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append(ch4);
			out.append(ch5);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append(ch4);
			out.append(ch5);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen4(ch0, ch1, ch2, ch3)
					+ DataOutputUTFUtils.utfLen2(ch4, ch5);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar4(out, ch0, ch1, ch2, ch3);
			DataOutputUTFUtils.writeUTFChar2(out, ch4, ch5);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return (hashCode4(ch0, ch1, ch2, ch3) * prime + ch4) * prime + ch5;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char6NodeName other = (Char6NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3
						&& ch4 == other.ch4 && ch5 == other.ch5;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char6NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 6 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 6) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char6NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), 
					name.charAt(4), name.charAt(5));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char7NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append(ch4);
			out.append(ch5);
			out.append(ch6);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append(ch4);
			out.append(ch5);
			out.append(ch6);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen4(ch0, ch1, ch2, ch3)
					+ DataOutputUTFUtils.utfLen3(ch4, ch5, ch6);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar4(out, ch0, ch1, ch2, ch3);
			DataOutputUTFUtils.writeUTFChar3(out, ch4, ch5, ch6);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return ((hashCode4(ch0, ch1, ch2, ch3) * prime + ch4) * prime + ch5) * prime + ch6;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char7NodeName other = (Char7NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char7NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 7 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 7) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char7NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), 
					name.charAt(4), name.charAt(5), name.charAt(6));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char8NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
		}

		@Override
		public int hashCode() {
			return hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char8NodeName other = (Char8NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char8NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 8 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 8) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char8NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char9NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append(ch8);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append(ch8);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7)
					+ DataOutputUTFUtils.utfLen(ch8);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar(out, ch8);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) * prime + ch8;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char9NodeName other = (Char9NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char9NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 9 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 9) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char9NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char10NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append(ch8);
			out.append(ch9);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append(ch8);
			out.append(ch9);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7)
					+ DataOutputUTFUtils.utfLen2(ch8, ch9);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar2(out, ch8, ch9);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return (hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) * prime + ch8) * prime + ch9;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char10NodeName other = (Char10NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char10NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 10 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 10) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char10NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char11NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;
		public final char ch10;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9 + ch10;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append(ch8);
			out.append(ch9);
			out.append(ch10);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append(ch8);
			out.append(ch9);
			out.append(ch10);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7)
					+ DataOutputUTFUtils.utfLen3(ch8, ch9, ch10);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar3(out, ch8, ch9, ch10);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return ((hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) * prime + ch8) * prime + ch9) * ch10;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char11NodeName other = (Char11NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char11NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 11 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 11) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char11NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9), name.charAt(10));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char12NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;
		public final char ch10;
		public final char ch11;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9 + ch10 + ch11;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7)
					+ DataOutputUTFUtils.utfLen4(ch8, ch9, ch10, ch11);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar4(out, ch8, ch9, ch10, ch11);
		}

		@Override
		public int hashCode() {
			return hashCode12(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7, ch8, ch9, ch10, ch11);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char12NodeName other = (Char12NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char12NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 12 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 12) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char12NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9), name.charAt(10), name.charAt(11));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char13NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;
		public final char ch10;
		public final char ch11;
		public final char ch12;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9 + ch10 + ch11 + ch12;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append(ch12);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append(ch12);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) 
					+ DataOutputUTFUtils.utfLen4(ch8, ch9, ch10, ch11)
					+ DataOutputUTFUtils.utfLen(ch12);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar4(out, ch8, ch9, ch10, ch11);
			DataOutputUTFUtils.writeUTFChar(out, ch12);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return (hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) * prime + hashCode4(ch8, ch9, ch10, ch11)) * prime + ch12;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char13NodeName other = (Char13NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char13NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 13 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 13) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char13NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9), name.charAt(10), name.charAt(11),
					name.charAt(12));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char14NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;
		public final char ch10;
		public final char ch11;
		public final char ch12;
		public final char ch13;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9 + ch10 + ch11 + ch12 + ch13;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append(ch12);
			out.append(ch13);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append(ch12);
			out.append(ch13);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) 
					+ DataOutputUTFUtils.utfLen4(ch8, ch9, ch10, ch11)
					+ DataOutputUTFUtils.utfLen2(ch12, ch13);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar4(out, ch8, ch9, ch10, ch11);
			DataOutputUTFUtils.writeUTFChar2(out, ch12, ch13);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			return (hashCode12(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7, ch8, ch9, ch10, ch11) * prime + ch12) * prime + ch13;
		}
			

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char14NodeName other = (Char14NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12 && ch13 == other.ch13;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char14NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 14 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 14) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char14NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9), name.charAt(10), name.charAt(11), //
					name.charAt(12), name.charAt(13));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char15NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;
		public final char ch10;
		public final char ch11;
		public final char ch12;
		public final char ch13;
		public final char ch14;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9 + ch10 + ch11 + ch12 + ch13 + ch14;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append(ch12);
			out.append(ch13);
			out.append(ch14);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append(ch12);
			out.append(ch13);
			out.append(ch14);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) 
					+ DataOutputUTFUtils.utfLen4(ch8, ch9, ch10, ch11)
					+ DataOutputUTFUtils.utfLen3(ch12, ch13, ch14);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar4(out, ch8, ch9, ch10, ch11);
			DataOutputUTFUtils.writeUTFChar3(out, ch12, ch13, ch14);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int hash12 = hashCode12(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7, ch8, ch9, ch10, ch11);
			return ((hash12 * prime + ch12) * prime + ch13) * prime + ch14;
		}
			

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char15NodeName other = (Char15NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12 && ch13 == other.ch13 && ch14 == other.ch14;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char15NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 15 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 15) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char15NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9), name.charAt(10), name.charAt(11), //
					name.charAt(12), name.charAt(13), name.charAt(14));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Char16NodeName extends NodeName {
		public final char ch0;
		public final char ch1;
		public final char ch2;
		public final char ch3;
		public final char ch4;
		public final char ch5;
		public final char ch6;
		public final char ch7;
		public final char ch8;
		public final char ch9;
		public final char ch10;
		public final char ch11;
		public final char ch12;
		public final char ch13;
		public final char ch14;
		public final char ch15;

		@Override
		public String toText() {
			return "" + ch0 + ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8 + ch9 + ch10 + ch11 + ch12 + ch13 + ch14 + ch15;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo8(out, ch8, ch9, ch10, ch11, ch12, ch13, ch14, ch15);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo8(out, ch8, ch9, ch10, ch11, ch12, ch13, ch14, ch15);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			int utfLen = DataOutputUTFUtils.utfLen8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) 
					+ DataOutputUTFUtils.utfLen8(ch8, ch9, ch10, ch11, ch12, ch13, ch14, ch15);
			DataOutputUTFUtils.writeUTFLen(out, utfLen);
			DataOutputUTFUtils.writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			DataOutputUTFUtils.writeUTFChar8(out, ch8, ch9, ch10, ch11, ch12, ch13, ch14, ch15);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int hash1 = hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			int hash2 = hashCode8(ch8, ch9, ch10, ch11, ch12, ch13, ch14, ch15);
			return hash1 * prime + hash2;
		}
			

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (getClass() == obj.getClass()) {
				Char16NodeName other = (Char16NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12 && ch13 == other.ch13 && ch14 == other.ch14 && ch15 == other.ch15;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Char16NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 16 * VM_CharSize
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 16) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Char16NodeName(name.charAt(0), name.charAt(1), name.charAt(2), name.charAt(3), // 
					name.charAt(4), name.charAt(5), name.charAt(6), name.charAt(7), //
					name.charAt(8), name.charAt(9), name.charAt(10), name.charAt(11), //
					name.charAt(12), name.charAt(13), name.charAt(14), name.charAt(15));
		}
	}
	
}
