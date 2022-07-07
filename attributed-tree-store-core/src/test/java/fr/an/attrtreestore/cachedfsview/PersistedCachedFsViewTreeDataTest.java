package fr.an.attrtreestore.cachedfsview;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.FileBlobStorage;

import fr.an.attrtreestore.TstMkdirDirUtils;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder;
import fr.an.attrtreestore.util.fsdata.helper.JavaNIONodeFsDataProvider;
import lombok.val;

public class PersistedCachedFsViewTreeDataTest {

	private static final File baseDir = TstMkdirDirUtils.initMkdir("target/test-data/PersistedCacheFsViewTreeData");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	private static final AttrInfoIndexes attrIndexes; 
	private static final NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault();
	private static final AttrDataEncoderHelper attrDataEncoder; 
	private static final IndexedBlobStorage_TreeNodeDataEncoder indexedEncoder;

	static {
		baseDir.mkdirs();
		attrIndexes = new AttrInfoIndexes(Collections.emptyList());
		attrDataEncoder = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder); 
		indexedEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoder);
	}

	
	@Test
	public void test1() {
		// Given
		String testDir = "test1";
		File test1Dir = TstMkdirDirUtils.initMkdir(baseDir, testDir);
		val scannedBaseDir = Paths.get("./src/test/data/rootDir1");
		String displayName = "tree1";
		String displayBaseUrl = "tree1";
		
		JavaNIONodeFsDataProvider nodeFsDataProvider = new JavaNIONodeFsDataProvider(nodeNameEncoder, scannedBaseDir);
		NodeFsAdapterOnDemandTreeData underlyingFsAdapterTree = new NodeFsAdapterOnDemandTreeData(nodeFsDataProvider);
		val sut = new PersistedCachedFsViewTreeData(displayName, displayBaseUrl,
				underlyingFsAdapterTree,
				blobStorage, testDir, // storage for persisted cached
				attrDataEncoder);
		val sutTree = sut.getTree();
		
		val dir1SubPath = nodeNameEncoder.encodePath("dir1");
	
		// When
		NodeData dir1Data = sut.getCacheWaitMax(dir1SubPath, 0, 0);

		// Then
		// => cache miss
		// Assert.assertEquals(1, sutTree.getCacheGetCount());
		Assert.assertEquals(0, sutTree.getCacheGetHitCounter());
		Assert.assertEquals(1, sutTree.getCacheGetMissCount());
		Assert.assertEquals(1, sutTree.getUnderlyingGetCount());
		Assert.assertNotNull(dir1Data);
		Assert.assertEquals(NodeData.TYPE_DIR, dir1Data.type);
		
		val dir1ChldLs = new ArrayList<>(dir1Data.childNames); // should contains "subdir1", "file1.txt"
		Assert.assertEquals(2, dir1ChldLs.size());
		val file1txtName = nodeNameEncoder.encode("file1.txt");
		val subdir1Name = nodeNameEncoder.encode("subdir1");
		Assert.assertEquals(file1txtName, dir1ChldLs.get(0));
		Assert.assertEquals(subdir1Name, dir1ChldLs.get(1));

		// When .. re-query same
		NodeData repeat_dir1Data = sut.getCacheWaitMax(dir1SubPath, 0, 0);

		// Then
		// => cache hit
		// Assert.assertEquals(2, sutTree.getCacheGetCount());
		Assert.assertEquals(1, sutTree.getCacheGetHitCount());
		Assert.assertEquals(1, sutTree.getCacheGetMissCount());
		Assert.assertEquals(1, sutTree.getUnderlyingGetCount());
		Assert.assertNotNull(repeat_dir1Data);
		Assert.assertTrue(dir1Data.equalsIgnoreTransientFields(repeat_dir1Data));

	}

}
