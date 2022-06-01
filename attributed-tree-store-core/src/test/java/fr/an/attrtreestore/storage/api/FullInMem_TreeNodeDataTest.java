package fr.an.attrtreestore.storage.api;

import java.io.File;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.spi.FileBlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.api.ReadOnlyCached_TreeNodeData.IndexedBlobStorageInitMode;
import fr.an.attrtreestore.storage.impl.Cached_ReadOnlyIndexedBlobStorage_TreeNodeData;
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder;
import lombok.Getter;
import lombok.val;

public class FullInMem_TreeNodeDataTest {
	
	private static final File baseDir = new File("target/test-data/PartialNodeDataByPathTest");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	private static final AttrInfoIndexes attrIndexes; 
	private static final NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault();
	private static AttrDataEncoderHelper attrDataEncoder; 
	private static final IndexedBlobStorage_TreeNodeDataEncoder indexedEncoder;
	
	private static long clockBackendStorageTime = 1; // emulate clock for 
	private static long clockModif = 1;

	
	private static final NodeName root = nodeNameEncoder.encode("");
	private static final NodeName a = nodeNameEncoder.encode("a");
	private static final NodeName b = nodeNameEncoder.encode("b");
	private static final NodeName c = nodeNameEncoder.encode("c");
	private static final NodeName d1 = nodeNameEncoder.encode("d1");
	private static final NodeName e1 = nodeNameEncoder.encode("e1");
	private static final NodeName d2 = nodeNameEncoder.encode("d2");
	private static final NodeName d3 = nodeNameEncoder.encode("d3");
	// private static final NodeNamesPath PATH_root = NodeNamesPath.of(); // ??
	private static final NodeNamesPath PATH_a = NodeNamesPath.of(a);
	private static final NodeNamesPath PATH_a_b = NodeNamesPath.of(a, b);
	private static final NodeNamesPath PATH_a_b_c = NodeNamesPath.of(a, b, c);
	private static final NodeNamesPath PATH_a_b_c_d1 = NodeNamesPath.of(a, b, c, d1);
	private static final NodeNamesPath PATH_a_b_c_d1_e1 = NodeNamesPath.of(a, b, c, d1, e1);
	private static final NodeNamesPath PATH_a_b_c_d2 = NodeNamesPath.of(a, b, c, d2);
	private static final NodeNamesPath PATH_a_b_c_d3 = NodeNamesPath.of(a, b, c, d3);

	static {
		baseDir.mkdirs();
		attrIndexes = new AttrInfoIndexes(Collections.emptyList());
		attrDataEncoder = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder); 
		indexedEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoder);
	}
	
	@Test
	public void test_put_recursiveWriteFull() {
		val sut = new FullInMem_TreeNodeData();
		
		// fill recursive some data..
		sut.put_root(createDirData(root, ImmutableSet.of(a)));
		
		val data_a = createDirData(a, ImmutableSet.of(b));
		sut.put_strictNoCreateParent(PATH_a, data_a);

		val data_a_b = createDirData(b, ImmutableSet.of(c));
		sut.put_strictNoCreateParent(PATH_a_b, data_a_b);
		
		NodeData data_a_b_c = createDirData(c, ImmutableSet.of(d1, d2, d3));
		sut.put_strictNoCreateParent(PATH_a_b_c, data_a_b_c);
		
		NodeData data_a_b_c_d1 = createDirData(d1, ImmutableSet.of(e1));
		sut.put_strictNoCreateParent(PATH_a_b_c_d1, data_a_b_c_d1);
		
		NodeData data_a_b_c_d1_e1 = createDirData(e1, ImmutableSet.of());
		sut.put_strictNoCreateParent(PATH_a_b_c_d1_e1, data_a_b_c_d1_e1);
		
		NodeData data_a_b_c_d2 = createDirData(d2, ImmutableSet.of());
		sut.put_strictNoCreateParent(PATH_a_b_c_d2, data_a_b_c_d2);
		
		NodeData data_a_b_c_d3 = createDirData(d3, ImmutableSet.of());
		sut.put_strictNoCreateParent(PATH_a_b_c_d3, data_a_b_c_d3);
		
		// write to file
		String fileName = "test-full-treedata1";
		sut.recursiveWriteFull(blobStorage, fileName, indexedEncoder);
		
		// re-read full file... and navigate + compare
		{
			val sutReload = new Cached_ReadOnlyIndexedBlobStorage_TreeNodeData(blobStorage, fileName, indexedEncoder, 
					IndexedBlobStorageInitMode.RELOAD_FULL, -1);
			
			get_assertDirData(data_a_b_c, sutReload, PATH_a_b_c);
			get_assertDirData(data_a_b_c_d1, sutReload, PATH_a_b_c_d1);
			get_assertDirData(data_a_b_c_d2, sutReload, PATH_a_b_c_d2);
			get_assertDirData(data_a_b_c_d3, sutReload, PATH_a_b_c_d3);
			get_assertDirData(data_a_b_c_d1_e1, sutReload, PATH_a_b_c_d1_e1);
			get_assertDirData(data_a_b_c, sutReload, PATH_a_b_c); // already queryed before  (lastQueryTime / lru may change)
		}
		
		// re-read only file with root .. and navigate with cache resolver + compare
		{
			val sutReload = new Cached_ReadOnlyIndexedBlobStorage_TreeNodeData(blobStorage, fileName, indexedEncoder, 
					IndexedBlobStorageInitMode.RELOAD_ROOT_ONLY, 100); // small size => load only 1 entry...

			get_assertDirData(data_a_b_c, sutReload, PATH_a_b_c); // .. load more (using defaultFetchSize => load all remaining?)
			get_assertDirData(data_a_b_c_d1, sutReload, PATH_a_b_c_d1);
			get_assertDirData(data_a_b_c_d2, sutReload, PATH_a_b_c_d2);
			get_assertDirData(data_a_b_c_d3, sutReload, PATH_a_b_c_d3);
			get_assertDirData(data_a_b_c_d1_e1, sutReload, PATH_a_b_c_d1_e1);
			get_assertDirData(data_a_b_c, sutReload, PATH_a_b_c); // already queryed before  (lastQueryTime / lru may change)
		}
	}

	private NodeData createDirData(NodeName name, ImmutableSet<NodeName> childNames) {
		return new NodeData(name,
			1, // type;
			0, // mask;
			childNames, 
			ImmutableMap.<String,NodeAttr>of(), // attrs
			clockBackendStorageTime++, // creationTime, 
			clockBackendStorageTime++, // lastModifiedTime,
			0L, // field1Long;
			clockModif++, // lastModifTimestamp;
			0, 0, 0L); // lruCount, lruAmortizedCount, lastQueryTimestamp
	}
	
	private static NodeData get_assertDirData(NodeData expected,
			Cached_ReadOnlyIndexedBlobStorage_TreeNodeData tree, 
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
