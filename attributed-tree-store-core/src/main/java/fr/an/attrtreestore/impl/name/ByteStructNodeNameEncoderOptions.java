package fr.an.attrtreestore.impl.name;

import static fr.an.attrtreestore.impl.name.NodeNameEncoderConstants.VM_ObjectHeaderSize;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.collect.ImmutableList;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.name.NodeNameEncoderOption;
import lombok.AllArgsConstructor;
import lombok.val;

/**
 * compression strategy for using embedded byte fields of small fixed size for ascii byte in range 00-7F, 
 * instead of "String" containing "Byte[]"
 *
 * .. see also similar compression, using Byte[]  
 */
public final class ByteStructNodeNameEncoderOptions {

	public static final ImmutableList<NodeNameEncoderOption> DEFAULTS = ImmutableList.<NodeNameEncoderOption>of(
			// FALLBACK,
			new Byte1NodeNameEncoderOption(),
			new Byte2NodeNameEncoderOption(),
			new Byte3NodeNameEncoderOption(),
			new Byte4NodeNameEncoderOption(),
			new Byte5NodeNameEncoderOption(),
			new Byte6NodeNameEncoderOption(),
			new Byte7NodeNameEncoderOption(),
			new Byte8NodeNameEncoderOption(),
			new Byte9NodeNameEncoderOption(),
			new Byte10NodeNameEncoderOption(),
			new Byte11NodeNameEncoderOption(),
			new Byte12NodeNameEncoderOption(),
			new Byte13NodeNameEncoderOption(),
			new Byte14NodeNameEncoderOption(),
			new Byte15NodeNameEncoderOption(),
			new Byte16NodeNameEncoderOption()
			);
	

	protected static boolean isSimpleChar(char c) {
		return (c >= 0x0001) && (c <= 0x007F);
	}

	protected static boolean isAllSimpleChars(String str) {
		int len = str.length();
		for(int i = 0; i < len; i++) {
			char ch = str.charAt(i);
			if (! isSimpleChar(ch)) {
				return false;
			}
		}
		return true;
	}

	protected static byte simpleCharAt(String str, int i) {
		return (byte) str.charAt(i);
	}
	
	// ------------------------------------------------------------------------
	
	public static abstract class ByteStructNodeName extends NodeName {
		
		
		protected static void appendTo4(StringBuilder out, byte ch0, byte ch1, byte ch2, byte ch3) {
			out.append((char) ch0);
			out.append((char) ch1);
			out.append((char) ch2);
			out.append((char) ch3);
		}
	
		protected static void appendTo4(PrintStream out, byte ch0, byte ch1, byte ch2, byte ch3) {
			out.append((char) ch0);
			out.append((char) ch1);
			out.append((char) ch2);
			out.append((char) ch3);
		}
	
		protected static int hashCode4(Byte ch0, byte ch1, byte ch2, byte ch3) {
			final int prime = 31;
			return ((ch0 * prime + ch1) * prime + ch2) * prime + ch3;
		}
	
		
		protected static void appendTo8(StringBuilder out, byte ch0, byte ch1, byte ch2, byte ch3, byte ch4, byte ch5, byte ch6, byte ch7) {
			out.append((char) ch0);
			out.append((char) ch1);
			out.append((char) ch2);
			out.append((char) ch3);
			out.append((char) ch4);
			out.append((char) ch5);
			out.append((char) ch6);
			out.append((char) ch7);
		}
	
		protected static void appendTo8(PrintStream out, byte ch0, byte ch1, byte ch2, byte ch3, byte ch4, byte ch5, byte ch6, byte ch7) {
			out.append((char) ch0);
			out.append((char) ch1);
			out.append((char) ch2);
			out.append((char) ch3);
			out.append((char) ch4);
			out.append((char) ch5);
			out.append((char) ch6);
			out.append((char) ch7);
		}
	
		protected static int hashCode8(Byte ch0, byte ch1, byte ch2, byte ch3, byte ch4, byte ch5, byte ch6, byte ch7) {
			final int prime = 31;
			return hashCode4(ch0, ch1, ch2, ch3) * prime + hashCode4(ch4, ch5, ch6, ch7);
		}
	
		protected static int hashCode12(Byte ch0, byte ch1, byte ch2, byte ch3, byte ch4, byte ch5, byte ch6, byte ch7,
				Byte ch8, byte ch9, byte ch10, byte ch11) {
			final int prime = 31;
			return hashCode8(ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7) * prime + hashCode4(ch8, ch9, ch10, ch11);
		}
	
		
		protected static void writeUTFChar(DataOutputStream out, byte c) throws IOException {
			out.write(c);
		}
		protected static void writeUTFChar2(DataOutputStream out, byte c0, byte c1) throws IOException {
			out.write(c0);
			out.write(c1);
		}
		protected static void writeUTFChar3(DataOutputStream out, byte c0, byte c1, byte c2) throws IOException {
			out.write(c0);
			out.write(c1);
			out.write(c2);
		}
		protected static void writeUTFChar4(DataOutputStream out, byte c0, byte c1, byte c2, byte c3) throws IOException {
			out.write(c0);
			out.write(c1);
			out.write(c2);
			out.write(c3);
		}
		protected static void writeUTFChar8(DataOutputStream out, byte c0, byte c1, byte c2, byte c3, byte c4, byte c5, byte c6, byte c7) throws IOException {
			out.write(c0);
			out.write(c1);
			out.write(c2);
			out.write(c3);
			out.write(c4);
			out.write(c5);
			out.write(c6);
			out.write(c7);
		}
		
		protected abstract int len();
		
		@Override
		public final String toText() {
			val sb = new StringBuilder(len());
			appendTo(sb);
			return sb.toString();
		}
	}
	
	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class Byte1NodeName extends ByteStructNodeName {
		public final byte ch0;

		@Override
		protected int len() {
			return 1;
		}

		@Override
		public void appendTo(StringBuilder out) {
			out.append((char) ch0);
		}

		@Override
		public void appendTo(PrintStream out) {
			out.append((char) ch0);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 1);
			writeUTFChar(out, ch0);
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
				Byte1NodeName other = (Byte1NodeName) obj;
				return ch0 == other.ch0;
			} else {
				return super.equals(obj);
			}
		}

	}
	
	public static class Byte1NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 1; // + VM_Padding7Size
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 1) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte1NodeName(simpleCharAt(name, 0));
		}
	}

	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class Byte2NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;

		@Override
		protected int len() {
			return 2;
		}

		@Override
		public void appendTo(StringBuilder out) {
			out.append((char) ch0);
			out.append((char) ch1);
		}

		@Override
		public void appendTo(PrintStream out) {
			out.append((char) ch0);
			out.append((char) ch1);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 2);
			writeUTFChar2(out, ch0, ch1);
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
				Byte2NodeName other = (Byte2NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte2NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 2; // + VM_Padding6Size
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 2) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte2NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1));
		}
	}

	// ------------------------------------------------------------------------
	
	@AllArgsConstructor
	public static class Byte3NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;

		@Override
		protected int len() {
			return 3;
		}

		@Override
		public void appendTo(StringBuilder out) {
			out.append((char) ch0);
			out.append((char) ch1);
			out.append((char) ch2);
		}

		@Override
		public void appendTo(PrintStream out) {
			out.append((char) ch0);
			out.append((char) ch1);
			out.append((char) ch2);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 3);
			writeUTFChar3(out, ch0, ch1, ch2);
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
				Byte3NodeName other = (Byte3NodeName) obj;
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
	
	public static class Byte3NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 3;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 3) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte3NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2));
		}
	}
	
	// ------------------------------------------------------------------------
	
	
	@AllArgsConstructor
	public static class Byte4NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;

		@Override
		protected int len() {
			return 4;
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
			DataOutputUTFUtils.writeUTFLen(out, 4);
			writeUTFChar4(out, ch0, ch1, ch2, ch3);
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
				Byte4NodeName p = (Byte4NodeName) obj;
				return ch0 == p.ch0 && ch1 == p.ch1 && ch2 == p.ch2 && ch3 == p.ch3;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte4NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 4;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 4) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte4NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte5NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;

		@Override
		protected int len() {
			return 5;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append((char) ch4);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append((char) ch4);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 5);
			writeUTFChar4(out, ch0, ch1, ch2, ch3);
			writeUTFChar(out, ch4);
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
				Byte5NodeName p = (Byte5NodeName) obj;
				return ch0 == p.ch0 && ch1 == p.ch1 && ch2 == p.ch2 && ch3 == p.ch3
						&& ch4 == p.ch4;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte5NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 5;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 5) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte5NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), simpleCharAt(name, 4));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte6NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;

		@Override
		protected int len() {
			return 6;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append((char) ch4);
			out.append((char) ch5);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append((char) ch4);
			out.append((char) ch5);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 6);
			writeUTFChar4(out, ch0, ch1, ch2, ch3);
			writeUTFChar2(out, ch4, ch5);
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
				Byte6NodeName other = (Byte6NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3
						&& ch4 == other.ch4 && ch5 == other.ch5;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte6NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 6;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 6) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte6NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), 
					simpleCharAt(name, 4), simpleCharAt(name, 5));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte7NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;

		@Override
		protected int len() {
			return 7;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append((char) ch4);
			out.append((char) ch5);
			out.append((char) ch6);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo4(out, ch0, ch1, ch2, ch3);
			out.append((char) ch4);
			out.append((char) ch5);
			out.append((char) ch6);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 7);
			writeUTFChar4(out, ch0, ch1, ch2, ch3);
			writeUTFChar3(out, ch4, ch5, ch6);
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
				Byte7NodeName other = (Byte7NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte7NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize
					+ 7
					;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 7) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte7NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte8NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;

		@Override
		protected int len() {
			return 8;
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
			DataOutputUTFUtils.writeUTFLen(out, 8);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
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
				Byte8NodeName other = (Byte8NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte8NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 8;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 8) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte8NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte9NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;

		@Override
		protected int len() {
			return 9;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append((char) ch8);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append((char) ch8);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 9);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar(out, ch8);
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
				Byte9NodeName other = (Byte9NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte9NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 9;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 9) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte9NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte10NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;

		@Override
		protected int len() {
			return 10;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append((char) ch8);
			out.append((char) ch9);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append((char) ch8);
			out.append((char) ch9);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 10);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar2(out, ch8, ch9);
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
				Byte10NodeName other = (Byte10NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte10NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 10;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 10) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte10NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte11NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;
		public final byte ch10;

		@Override
		protected int len() {
			return 11;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append((char) ch8);
			out.append((char) ch9);
			out.append((char) ch10);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			out.append((char) ch8);
			out.append((char) ch9);
			out.append((char) ch10);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 11);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar3(out, ch8, ch9, ch10);
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
				Byte11NodeName other = (Byte11NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte11NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 11;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 11) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte11NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9), simpleCharAt(name, 10));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte12NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;
		public final byte ch10;
		public final byte ch11;

		@Override
		protected int len() {
			return 12;
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
			DataOutputUTFUtils.writeUTFLen(out, 12);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar4(out, ch8, ch9, ch10, ch11);
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
				Byte12NodeName other = (Byte12NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte12NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 12;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 12) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte12NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9), simpleCharAt(name, 10), simpleCharAt(name, 11));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte13NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;
		public final byte ch10;
		public final byte ch11;
		public final byte ch12;

		@Override
		protected int len() {
			return 13;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append((char) ch12);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append((char) ch12);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 13);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar4(out, ch8, ch9, ch10, ch11);
			writeUTFChar(out, ch12);
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
				Byte13NodeName other = (Byte13NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte13NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 13;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 13) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte13NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9), simpleCharAt(name, 10), simpleCharAt(name, 11),
					simpleCharAt(name, 12));
		}
	}

	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte14NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;
		public final byte ch10;
		public final byte ch11;
		public final byte ch12;
		public final byte ch13;

		@Override
		protected int len() {
			return 14;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append((char) ch12);
			out.append((char) ch13);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append((char) ch12);
			out.append((char) ch13);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 14);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar4(out, ch8, ch9, ch10, ch11);
			writeUTFChar2(out, ch12, ch13);
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
				Byte14NodeName other = (Byte14NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12 && ch13 == other.ch13;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte14NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 14;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 14) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte14NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9), simpleCharAt(name, 10), simpleCharAt(name, 11), //
					simpleCharAt(name, 12), simpleCharAt(name, 13));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte15NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;
		public final byte ch10;
		public final byte ch11;
		public final byte ch12;
		public final byte ch13;
		public final byte ch14;

		@Override
		protected int len() {
			return 15;
		}

		@Override
		public void appendTo(StringBuilder out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append((char) ch12);
			out.append((char) ch13);
			out.append((char) ch14);
		}

		@Override
		public void appendTo(PrintStream out) {
			appendTo8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			appendTo4(out, ch8, ch9, ch10, ch11);
			out.append((char) ch12);
			out.append((char) ch13);
			out.append((char) ch14);
		}

		@Override
		public void writeUTF(DataOutputStream out) throws IOException {
			DataOutputUTFUtils.writeUTFLen(out, 15);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar4(out, ch8, ch9, ch10, ch11);
			writeUTFChar3(out, ch12, ch13, ch14);
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
				Byte15NodeName other = (Byte15NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12 && ch13 == other.ch13 && ch14 == other.ch14;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte15NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 15;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 15) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte15NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9), simpleCharAt(name, 10), simpleCharAt(name, 11), //
					simpleCharAt(name, 12), simpleCharAt(name, 13), simpleCharAt(name, 14));
		}
	}


	// ------------------------------------------------------------------------

	@AllArgsConstructor
	public static class Byte16NodeName extends ByteStructNodeName {
		public final byte ch0;
		public final byte ch1;
		public final byte ch2;
		public final byte ch3;
		public final byte ch4;
		public final byte ch5;
		public final byte ch6;
		public final byte ch7;
		public final byte ch8;
		public final byte ch9;
		public final byte ch10;
		public final byte ch11;
		public final byte ch12;
		public final byte ch13;
		public final byte ch14;
		public final byte ch15;

		@Override
		protected int len() {
			return 16;
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
			DataOutputUTFUtils.writeUTFLen(out, 16);
			writeUTFChar8(out, ch0, ch1, ch2, ch3, ch4, ch5, ch6, ch7);
			writeUTFChar8(out, ch8, ch9, ch10, ch11, ch12, ch13, ch14, ch15);
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
				Byte16NodeName other = (Byte16NodeName) obj;
				return ch0 == other.ch0 && ch1 == other.ch1 && ch2 == other.ch2 && ch3 == other.ch3 //
						&& ch4 == other.ch4 && ch5 == other.ch5 && ch6 == other.ch6 && ch7 == other.ch7 //
						&& ch8 == other.ch8 && ch9 == other.ch9 && ch10 == other.ch10 && ch11 == other.ch11 //
						&& ch12 == other.ch12 && ch13 == other.ch13 && ch14 == other.ch14 && ch15 == other.ch15;
			} else {
				return super.equals(obj);
			}
		}
		
	}
	
	public static class Byte16NodeNameEncoderOption extends NodeNameEncoderOption {
		public static final int ENCODER_SIZE = VM_ObjectHeaderSize + 16;
		
		@Override
		public NodeName tryEncode(String name, int maxEstimatedSize, EncodeSizeResult sizeResult) {
			if (name.length() != 16) {
				return null;
			}
			if (! isAllSimpleChars(name)) {
				return null;
			}
			sizeResult.estimatedSize = ENCODER_SIZE;
			return new Byte16NodeName(simpleCharAt(name, 0), simpleCharAt(name, 1), simpleCharAt(name, 2), simpleCharAt(name, 3), // 
					simpleCharAt(name, 4), simpleCharAt(name, 5), simpleCharAt(name, 6), simpleCharAt(name, 7), //
					simpleCharAt(name, 8), simpleCharAt(name, 9), simpleCharAt(name, 10), simpleCharAt(name, 11), //
					simpleCharAt(name, 12), simpleCharAt(name, 13), simpleCharAt(name, 14), simpleCharAt(name, 15));
		}
	}
	
}
