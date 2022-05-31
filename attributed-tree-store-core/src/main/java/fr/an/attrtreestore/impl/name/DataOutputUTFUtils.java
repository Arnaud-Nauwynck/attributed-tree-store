package fr.an.attrtreestore.impl.name;

import java.io.DataOutputStream;
import java.io.IOException;

/*pp*/ final class DataOutputUTFUtils {

	public static int utfLen(char c) {
        if ((c >= 0x0001) && (c <= 0x007F)) {
            return 1;
        } else if (c > 0x07FF) {
            return 3;
        } else {
            return 2;
        }
	}
	
	public static void writeUTFLen(DataOutputStream out, int utflen) throws IOException {
		out.write((byte) ((utflen >>> 8) & 0xFF));
		out.write((byte) ((utflen >>> 0) & 0xFF));
	}

	public static void writeUTFChar(DataOutputStream out, char c) throws IOException {
		if ((c >= 0x0001) && (c <= 0x007F)) {
			out.write((byte) c);
        } else if (c > 0x07FF) {
            out.write((byte) (0xE0 | ((c >> 12) & 0x0F)));
            out.write((byte) (0x80 | ((c >>  6) & 0x3F)));
            out.write((byte) (0x80 | ((c >>  0) & 0x3F)));
        } else {
            out.write((byte) (0xC0 | ((c >>  6) & 0x1F)));
            out.write((byte) (0x80 | ((c >>  0) & 0x3F)));
        }
	}

	// ------------------------------------------------------------------------
	
	public static int utfLen2(char c0, char c1) {
		return utfLen(c0) + utfLen(c1); 
	}
	public static void writeUTFChar2(DataOutputStream out, char c0, char c1) throws IOException {
		writeUTFChar(out, c0);
		writeUTFChar(out, c1);
	}

	public static int utfLen3(char c0, char c1, char c2) {
		return utfLen(c0) + utfLen(c1) + utfLen(c2); 
	}
	public static void writeUTFChar3(DataOutputStream out, char c0, char c1, char c2) throws IOException {
		writeUTFChar(out, c0);
		writeUTFChar(out, c1);
		writeUTFChar(out, c2);
	}

	public static int utfLen4(char c0, char c1, char c2, char c3) {
		return utfLen(c0) + utfLen(c1) + utfLen(c2) + utfLen(c3); 
	}
	public static void writeUTFChar4(DataOutputStream out, char c0, char c1, char c2, char c3) throws IOException {
		writeUTFChar(out, c0);
		writeUTFChar(out, c1);
		writeUTFChar(out, c2);
		writeUTFChar(out, c3);
	}


	public static int utfLen8(char c0, char c1, char c2, char c3, char c4, char c5, char c6, char c7) {
		return utfLen4(c0, c1, c2, c3) + utfLen4(c4, c5, c6, c7); 
	}
	public static void writeUTFChar8(DataOutputStream out, char c0, char c1, char c2, char c3, char c4, char c5, char c6, char c7) throws IOException {
		writeUTFChar4(out, c0, c1, c2, c3);
		writeUTFChar4(out, c4, c5, c6, c7);
	}

}
