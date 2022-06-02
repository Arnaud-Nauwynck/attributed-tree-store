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
import fr.an.attrtreestore.api.ROCached_TreeData.IndexedBlobStorageInitMode;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.attrinfo.AttrInfoRegistry;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import fr.an.attrtreestore.impl.traversal.CounterNodeTreeDataVisitor;
import fr.an.attrtreestore.impl.traversal.CounterNodeTreeDataVisitor.TreeCount;
import fr.an.attrtreestore.impl.traversal.DefaultTreeDataWalker;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.spi.FileBlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.impl.CachedROIndexedBlobStorage_TreeNodeData;
import fr.an.attrtreestore.storage.impl.InMem_TreeData;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JavaNIOScan_WriteFile_ReadFile_IT {

	private static NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault();
	private static final AttrInfoRegistry attrRegistry = new AttrInfoRegistry();
	private static final AttrInfoIndexes attrIndexes = new AttrInfoIndexes(new ArrayList<>());
	private static final AttrDataEncoderHelper attrDataEncoderHelper = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder);
	private static final IndexedBlobStorage_TreeNodeDataEncoder treeDataEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoderHelper); 
	private static final File baseDir = TstMkdirDirUtils.initMkdir("target/test-data/JavaNIOScan_WriteFile_ReadFile_IT");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	
	@Test
	public void testWriteRecursiveNode_rootDir1() throws Exception {
		val outputFile = "testTree1.dat";
		val scannedDir = Paths.get("./src/test/data/rootDir1");
		doScan_WriteFile_ReadFile_Compare(outputFile, scannedDir);
	}

	@Test
	public void testWriteRecursiveNode_m2Repo() throws Exception {
		val userHome = System.getProperty("user.home");
		val path = Paths.get(userHome + "/.m2");
		
		val outputFile = "testTree_m2.dat";
		doScan_WriteFile_ReadFile_Compare(outputFile, path);
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
			log.info("scan " + child + " ...");
			val outputFile = "testTree_c_" + name + ".dat";
			try {
				doScan_WriteFile_ReadFile_Compare(outputFile, child);
			} catch(Exception ex) {
				log.info("Failed scan " + child + ": " + ex.getMessage());
			}
		}
	}

	// ------------------------------------------------------------------------


	private void doScan_WriteFile_ReadFile_Compare(String outputFile, Path scanRootPath) throws Exception {
		System.gc();
		Thread.sleep(10);
		System.gc();
		val memBean = ManagementFactory.getMemoryMXBean();
		val heapBefore = memBean.getHeapMemoryUsage().getUsed();

		log.info("scan local filesystem '" + scanRootPath + "' ...");

		long startTime = System.currentTimeMillis();
		
		val tree = scan(scanRootPath);
		
		long millis = System.currentTimeMillis() - startTime;
		log.info("done scanned local filestystem '" + scanRootPath + "', took " + millis + " ms");

		System.gc();
		Thread.sleep(10);
		System.gc();
		val heapAfter = memBean.getHeapMemoryUsage().getUsed();
		val usedMem = heapAfter - heapBefore;
		
		val countTree = countScanTree(tree);
		
		val countTotal = (countTree.getCount() != 0)? countTree.getCount() : 1;
		log.info("diff used memory: " + usedMem + " = " + (usedMem/1024/1024) + " MB"
				+ " = " + (usedMem/countTotal) + " b/u (count:" + countTotal + ")"
				);

		doWriteFile(outputFile, tree);

		val reloadTree = doReadFile(attrRegistry, outputFile);
				
		val countReload = countScanTree(reloadTree);
		// TOADD compare count
		Assert.assertEquals(countTree.getType1Count(), countReload.getType1Count());
		Assert.assertEquals(countTree.getType2Count(), countReload.getType2Count());
		Assert.assertEquals(countTree.getCount(), countReload.getCount());
		
		// TOADD
		// doCompareTree(tree, reloadTree);

	}

	protected static InMem_TreeData scan(Path scannedDir) { 
		val tree = new InMem_TreeData();
		val converter = new NodeFsDataToNodeDataConverter(System.currentTimeMillis());
		val builder = new FullInMemTree_NodeFsDataCreator(converter, tree);
		JavaNIOFileToNodeFsDataScanner.scan(scannedDir, nodeNameEncoder, builder);
		return tree;
	}


	private void doWriteFile(String outputFile, InMem_TreeData tree) throws IOException, FileNotFoundException {
		log.info("write to indexed file: " + outputFile);
		val startMillis = System.currentTimeMillis();

		tree.recursiveWriteFull(blobStorage, outputFile, treeDataEncoder);

		// long fileLen;
		val millis = System.currentTimeMillis() - startMillis;
		
		long fileLen = blobStorage.fileLen(outputFile);
		log.info(".. done write to indexed file " 
				+ " => " + (fileLen/1024) + " kb" 
				+ ((fileLen>1024*1024)? " = " + (fileLen/1024/1024) + " MB" : "") 
				+ ", took " + millis + " ms");
	}

	private CachedROIndexedBlobStorage_TreeNodeData doReadFile(AttrInfoRegistry attrRegistry, String file) throws IOException, FileNotFoundException {
		log.info("reload cacheTree from indexed file (preload FULL):" + file);
		val startMillis = System.currentTimeMillis();
		
		val res = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, file, treeDataEncoder, 
				IndexedBlobStorageInitMode.RELOAD_FULL, -1);
		Assert.assertNotNull(res);
		
		val millis = System.currentTimeMillis() - startMillis;
		log.info(".. done reload cacheTree from indexed file (preload FULL), took " + millis + " ms");

		return res;
	}
	
	
	private TreeCount countScanTree(TreeData tree) {
		// recursive scan to count + check valid
		val startMillis = System.currentTimeMillis();
		val countVisitor = new CounterNodeTreeDataVisitor();
		val walker = new DefaultTreeDataWalker<Void>(tree, countVisitor);
		
		walker.visitRecursiveRoot();
		val res = countVisitor.getCounter();
		
		val millis = System.currentTimeMillis() - startMillis;
		val dirCount = res.getType1Count();
		val fileCount = res.getType2Count();
		// val dirNotEmptyCount = res.getChildListCount();
		log.info("scan count tree => " + dirCount + " dirs + " + fileCount + " files"
				+ ", took " + millis + " ms");

		return res;
	}

}
