package fr.an.attrtreestore.util.fsdata.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import fr.an.attrtreestore.TstMkdirDirUtils;
import fr.an.attrtreestore.api.FullInMem_TreeData;
import fr.an.attrtreestore.api.ROCached_TreeData.IndexedBlobStorageInitMode;
import fr.an.attrtreestore.api.attrinfo.AttrInfoRegistry;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.spi.FileBlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.impl.CachedROIndexedBlobStorage_TreeNodeData;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder;
import lombok.val;

public class JavaNIOScan_WriteFile_ReadFile_IT {

	private static NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault();
	private static final AttrInfoRegistry attrRegistry = new AttrInfoRegistry();
	private static final AttrInfoIndexes attrIndexes = new AttrInfoIndexes(new ArrayList<>());
	private static final AttrDataEncoderHelper attrDataEncoderHelper = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder);
	private static final IndexedBlobStorage_TreeNodeDataEncoder treeDataEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoderHelper); 
	private static final File baseDir = TstMkdirDirUtils.initMkdir("target/test-data/JavaNIOScan_WriteFile_ReadFile_IT");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	
	static {
		// warmup for bench?
	}
	
	protected static FullInMem_TreeData scan(Path scannedDir) { 
		val tree = new FullInMem_TreeData();
		val converter = new NodeFsDataToNodeDataConverter(System.currentTimeMillis());
		val builder = new FullInMemTree_NodeFsDataCreator(converter, tree);
		JavaNIOFileToNodeFsDataScanner.scan(scannedDir, nodeNameEncoder, builder);
		return tree;
	}


	@Test
	public void testWriteRecursiveNode_rootDir1() throws Exception {
		val tree = scan(Paths.get("./src/test/data/rootDir1"));
		
		val file = "testTree1.dat";
		doWriteFile(file, tree);
		doReadFile(attrRegistry, file);
	}

//	private void countNodes(Node rootNode) {
//		val counter = new NodeFsDataCounter();
//		rootNode.accept(counter);
//		val countFile = counter.getCountFile();
//		val countDir = counter.getCountDir();
//		val countTotal = countFile + countDir;
//		
//		System.out.println("count: " + countTotal + "(files:" + countFile + ", dirs:" + countDir + ")");
//	}
	
	private void doWriteFile(String outputFile, FullInMem_TreeData tree) throws IOException, FileNotFoundException {
		System.out.println("write to file: " + outputFile);
		val startMillis = System.currentTimeMillis();

		tree.recursiveWriteFull(blobStorage, outputFile, treeDataEncoder);

		// long fileLen;
		val millis = System.currentTimeMillis() - startMillis;
		System.out.println(".. done write => " 
				//	+ (fileLen/1024) + " kb"
				+ ", took " + millis + " ms");
	}

	private void doReadFile(AttrInfoRegistry attrRegistry, String file) throws IOException, FileNotFoundException {
		System.out.println("read from file: " + file);
		val startMillis = System.currentTimeMillis();
		
		val cachedRO = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, file, treeDataEncoder, 
				IndexedBlobStorageInitMode.RELOAD_FULL, -1);
		Assert.assertNotNull(cachedRO);
		
		val millis = System.currentTimeMillis() - startMillis;
		System.out.println(".. done read file:" + file + ", took " + millis + " ms");
	}
	
	@Test
	public void testWriteRecursiveNode_m2Repo() throws Exception {
		val userHome = System.getProperty("user.home");
		val path = Paths.get(userHome + "/.m2");
		System.out.println("scan " + path + " ...");
		
		val outputFile = "testTree_m2.dat";
		doScanWriteRecursiveNode(outputFile, path);
	}

	@Test @Ignore
	public void testWriteRecursiveNode_c() throws Exception {
		val path = Paths.get("c:\\"); // => AccesDeniedException "c:\\$Recycle.Bin" ?!
		val childList = Files.list(path).collect(Collectors.toList());
		for(val child : childList) {
			val name = child.getFileName().toString();
			if (name.endsWith("$Recycle.Bin")) {
				continue;
			}
			System.out.println("scan " + child + " ...");
			val outputFile = "testTree_c_" + name + ".dat";
			try {
				doScanWriteRecursiveNode(outputFile, child);
			} catch(Exception ex) {
				System.out.println("Failed scan " + child + ": " + ex.getMessage());
			}
		}
	}

	private void doScanWriteRecursiveNode(String outputFile, Path scanRootPath) throws Exception {
		System.gc();
		val memBean = ManagementFactory.getMemoryMXBean();
		val heapBefore = memBean.getHeapMemoryUsage().getUsed();
		
		long startTime = System.currentTimeMillis();
		
		val rootNode = scan(scanRootPath);
		
		long millis = System.currentTimeMillis() - startTime;
		System.out.println("done scanned " + scanRootPath + ", took " + millis + "ms");

		System.gc();
		val heapAfter = memBean.getHeapMemoryUsage().getUsed();
		val usedMem = heapAfter - heapBefore;

//		val counter = new NodeFsDataCounter();
//		rootNode.accept(counter);
//		val countFile = counter.getCountFile();
//		val countDir = counter.getCountDir();
//		val countTotal = countFile + countDir;
		
		System.out.println(// "count: " + countTotal + "(files:" + countFile + ", dirs:" + countDir + ")" +
				" used memory: " + usedMem
				// + " (=" + (usedMem/countTotal) + " b/u)"
				);

		doWriteFile(outputFile, rootNode);

		doReadFile(attrRegistry, outputFile);
	}

}
