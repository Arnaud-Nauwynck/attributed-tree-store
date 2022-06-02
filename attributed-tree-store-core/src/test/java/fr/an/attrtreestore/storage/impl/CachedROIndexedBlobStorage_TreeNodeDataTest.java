package fr.an.attrtreestore.storage.impl;

import java.io.File;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData.IndexedBlobStorageInitMode;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.spi.FileBlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.api.TreeTstObj;
import lombok.val;

public class CachedROIndexedBlobStorage_TreeNodeDataTest {
	
	private static final File baseDir = new File("target/test-data/CachedReadOnlyIndexed");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	private static final AttrInfoIndexes attrIndexes; 
	private static final NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault();
	private static AttrDataEncoderHelper attrDataEncoder; 
	private static final IndexedBlobStorage_TreeNodeDataEncoder indexedEncoder;

	static {
		baseDir.mkdirs();
		attrIndexes = new AttrInfoIndexes(Collections.emptyList());
		attrDataEncoder = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder); 
		indexedEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoder);
	}
	
	@Test
	public void test_put_recursiveWriteFull() {
		val src = new TreeTstObj();
		
		// write to file
		String fileName = "test-full-treedata1";
		src.treeData.recursiveWriteFull(blobStorage, fileName, indexedEncoder);
		
		// re-read full file... and navigate + compare
		{
			val sutReload = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, fileName, indexedEncoder, 
					IndexedBlobStorageInitMode.RELOAD_FULL, -1);
			
			val cacheHit0 = sutReload.getCacheHit();
			val cacheMiss0 = sutReload.getCacheMiss();
			Assert.assertEquals(0, cacheHit0);
			Assert.assertEquals(0, cacheMiss0);
			
			get_assertDirData(src.data_a_b_c, sutReload, TreeTstObj.PATH_a_b_c);

			get_assertDirData(src.data_a_b_c_d1, sutReload, TreeTstObj.PATH_a_b_c_d1);
			get_assertDirData(src.data_a_b_c_d2, sutReload, TreeTstObj.PATH_a_b_c_d2);
			get_assertDirData(src.data_a_b_c_d3, sutReload, TreeTstObj.PATH_a_b_c_d3);
			get_assertDirData(src.data_a_b_c_d1_e1, sutReload, TreeTstObj.PATH_a_b_c_d1_e1);
			get_assertDirData(src.data_a_b_c, sutReload, TreeTstObj.PATH_a_b_c); // already queryed before  (lastQueryTime / lru may change)

			val cacheHit1 = sutReload.getCacheHit();
			Assert.assertTrue(cacheHit1 >= 23);
			val cacheMiss1 = sutReload.getCacheMiss();
			Assert.assertEquals(0, cacheMiss1);

		}
		
		// re-read only file with root .. and navigate with cache resolver + compare
		{
			val sutReload = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, fileName, indexedEncoder, 
					IndexedBlobStorageInitMode.RELOAD_ROOT_ONLY, 100); // small size => load only 1 entry...

			get_assertDirData(src.data_a_b_c, sutReload, TreeTstObj.PATH_a_b_c);
			val cacheHit1 = sutReload.getCacheHit();
			val cacheMiss1 = sutReload.getCacheMiss();
			Assert.assertEquals(2, cacheHit1);
			Assert.assertEquals(1, cacheMiss1);
			
			get_assertDirData(src.data_a_b_c_d1, sutReload, TreeTstObj.PATH_a_b_c_d1);
			val cacheHit2 = sutReload.getCacheHit();
			val cacheMiss2 = sutReload.getCacheMiss();
			Assert.assertEquals(6, cacheHit2);
			Assert.assertEquals(cacheMiss1, cacheMiss2);

			get_assertDirData(src.data_a_b_c_d2, sutReload, TreeTstObj.PATH_a_b_c_d2);
			get_assertDirData(src.data_a_b_c_d3, sutReload, TreeTstObj.PATH_a_b_c_d3);
			get_assertDirData(src.data_a_b_c_d1_e1, sutReload, TreeTstObj.PATH_a_b_c_d1_e1);
			get_assertDirData(src.data_a_b_c, sutReload, TreeTstObj.PATH_a_b_c); // already queryed before  (lastQueryTime / lru may change)

			val cacheHit3 = sutReload.getCacheHit();
			val cacheMiss3 = sutReload.getCacheMiss();
			Assert.assertTrue(cacheHit3 >= 22);
			Assert.assertEquals(cacheMiss1, cacheMiss3);
		}
	}

	private static NodeData get_assertDirData(NodeData expected,
			CachedROIndexedBlobStorage_TreeNodeData tree, 
			NodeNamesPath path) {
		val actual = tree.get(path);
		Assert.assertNotNull(actual);
		Assert.assertEquals(expected.name, path.lastName());
		Assert.assertEquals(expected.name, actual.name);
		
		Assert.assertEquals(expected.type, actual.type);
		Assert.assertEquals(expected.mask, actual.mask);
		// TOADD assertEquals childNames; 
		// TOADD assertEquals attrs;

		Assert.assertEquals(expected.creationTime, actual.creationTime);
		Assert.assertEquals(expected.lastModifiedTime, actual.lastModifiedTime);
		Assert.assertEquals(expected.field1Long, actual.field1Long);
		Assert.assertEquals(expected.getLastModifTimestamp(), actual.getLastModifTimestamp());
		// lruCount... may differ 
		
		return actual;
	}
}
