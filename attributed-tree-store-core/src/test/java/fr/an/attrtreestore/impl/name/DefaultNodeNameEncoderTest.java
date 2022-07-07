package fr.an.attrtreestore.impl.name;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.path4j.NodeName;
import org.path4j.encoder.DefaultNodeNameEncoder;

public class DefaultNodeNameEncoderTest {

	@Test
	public void testEncodeAscii() throws IOException {
		DefaultNodeNameEncoder sut = DefaultNodeNameEncoder.createDefault();
		
		Random rand = new Random(0);
		for(int len = 1; len < 20; len++) {
			// generate String of length 'len'
			String name = generateAsciiNameLen(rand, len);
			Assert.assertEquals(name.length(), len);
			
			doTestName(sut, name);
		}
	}

	@Test
	public void testEncodeUtf() throws IOException {
		DefaultNodeNameEncoder sut = DefaultNodeNameEncoder.createDefault();
		
		Random rand = new Random(0);
		for(int len = 1; len < 20; len++) {
			String nameUtf = generateUtfNameLen(rand, len);
			Assert.assertEquals(nameUtf.length(), len);
			
			doTestName(sut, nameUtf);
		}
	}
	
	private static String generateAsciiNameLen(Random rand, int len) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < len; i++) {
			char ch = (char) ('a' + rand.nextInt(26));
			sb.append(ch);
		}
		String name = sb.toString();
		return name;
	}

	private static String generateUtfNameLen(Random rand, int len) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < len; i++) {
			char ch = (char) (0x07FF + rand.nextInt(100));
			sb.append(ch);
		}
		String name = sb.toString();
		return name;
	}

	
	private void doTestName(DefaultNodeNameEncoder sut, String name) throws IOException {
		// memory encoding
		NodeName nodeName = sut.encode(name);
		
		// check toText()
		String checkName = nodeName.toText();
		Assert.assertEquals(checkName, name);
		
		// check appendTo()
		StringBuilder checkSb = new StringBuilder();
		nodeName.appendTo(checkSb);
		Assert.assertEquals(checkSb.toString(), name);

		ByteArrayOutputStream checkDataBuffer = new ByteArrayOutputStream();
		try (DataOutputStream dataOut = new DataOutputStream(checkDataBuffer)) {
			nodeName.writeUTF(dataOut);
		}
		String checkDataUTFName;
		try (DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(checkDataBuffer.toByteArray()))) {
			checkDataUTFName = dataIn.readUTF();
		}
		Assert.assertEquals(checkDataUTFName, name);

		ByteArrayOutputStream printStreamBuffer = new ByteArrayOutputStream();
		try (PrintStream printOut = new PrintStream(printStreamBuffer)) {
			printOut.print(name);
		}
		String printStreamName = printStreamBuffer.toString();
		// boolean isPrintSame = printStreamName.equals(name); // ??? 
		// ... wrong PrintStream change name.. Assert.assertEquals(printStreamName, name);

		ByteArrayOutputStream checkPrintStreamBuffer = new ByteArrayOutputStream();
		try (PrintStream printOut = new PrintStream(checkPrintStreamBuffer)) {
			nodeName.appendTo(printOut);
		}
		String checkPrintStreamName = checkPrintStreamBuffer.toString();
		Assert.assertEquals(checkPrintStreamName, printStreamName);

		// check equals()
		NodeName nodeName2 = sut.encode(name);
		Assert.assertEquals(nodeName, nodeName2);

		// check hashCode()
		Assert.assertEquals(nodeName.hashCode(), nodeName2.hashCode());
	}

}
