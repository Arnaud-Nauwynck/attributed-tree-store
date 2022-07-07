package fr.an.attrtreestore.util.fsdata.helper;

import java.nio.file.Paths;

import org.junit.Test;
import org.path4j.encoder.DefaultNodeNameEncoder;

import lombok.val;

public class JavaNIOFileToNodeFsDataScannerTest {

	@Test
	public void testScan_data_rootDir1() {
		val path = Paths.get("./src/test/data/rootDir1");
		val nodeNameEncoder = DefaultNodeNameEncoder.createDefault(); 
		val callback = new NodeFsDataCounter();
		JavaNIOFileToNodeFsDataScanner.scan(path, nodeNameEncoder, callback);
		System.out.println("done scan => " + callback.getCountFile() + " files, " + callback.getCountDir() + " dirs");
	}
}
