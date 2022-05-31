package fr.an.attrtreestore.storage.impl;

import java.io.File;
import java.util.Collections;
import java.util.function.Supplier;

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
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.api.NodeOverrideStatus;
import lombok.val;

public class AppendOnlyBlobStorage_PartialNodeDataByPathTest {

	private static final File baseDir = new File("target/test-data/PartialNodeDataByPathTest");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	private static final AttrInfoIndexes attrIndexes; 
	private static final NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault(); 
	
	private static long clockBackendStorageTime = 1; // emulate clock for 
	private static long clockModif = 1;

	private static final NodeNamesPath PATH_a_b_c = nodeNameEncoder.encodePath("a/b/c");
	private static final NodeNamesPath PATH_a_b_c_d1 = nodeNameEncoder.encodePath("a/b/c/d1");
	private static final NodeNamesPath PATH_a_b_c_d1_e1 = nodeNameEncoder.encodePath("a/b/c/d1/e1");
	private static final NodeNamesPath PATH_a_b_c_d3 = nodeNameEncoder.encodePath("a/b/c/d3");
	private static final NodeName a = nodeNameEncoder.encode("a");
	private static final NodeName b = nodeNameEncoder.encode("b");
	private static final NodeName c = nodeNameEncoder.encode("c");
	private static final NodeName d1 = nodeNameEncoder.encode("d1");
	private static final NodeName d2 = nodeNameEncoder.encode("d2");
	
	static {
		baseDir.mkdirs();
		attrIndexes = new AttrInfoIndexes(Collections.emptyList()); 
	}
			
	@Test
	public void test_put_get_remove() {
		val sut = new AppendOnlyBlobStorage_PartialNodeDataByPath(
				blobStorage, "test1", attrIndexes, nodeNameEncoder);
		sut.initCreateEmpty();
		
		Supplier<AppendOnlyBlobStorage_PartialNodeDataByPath> reloader = () -> reloadFromFile("test1");
		
		// -- Step 1 --
		NodeData data1_a_b_c = createDirData(c, ImmutableSet.of(d1, d2));
		sut.put(PATH_a_b_c, data1_a_b_c);

		{
			// assert in-memory
			val res1_a_b_c = sut.get(PATH_a_b_c);
			Assert.assertEquals(NodeOverrideStatus.UPDATED, res1_a_b_c.status);
	
			val res1_a_b_c_d1 = sut.get(PATH_a_b_c_d1);
			Assert.assertEquals(NodeOverrideStatus.NOT_OVERRIDEN, res1_a_b_c_d1.status);
	
			val res1_a_b_c_d1_e1 = sut.get(PATH_a_b_c_d1_e1);
			Assert.assertEquals(NodeOverrideStatus.NOT_OVERRIDEN, res1_a_b_c_d1_e1.status);

			// assert using reload
			val sutReload = reloader.get();

			val reloadres1_a_b_c = sutReload.get(PATH_a_b_c);
			Assert.assertEquals(NodeOverrideStatus.UPDATED, reloadres1_a_b_c.status);
	
			val reloadres1_a_b_c_d1 = sutReload.get(PATH_a_b_c_d1);
			Assert.assertEquals(NodeOverrideStatus.NOT_OVERRIDEN, reloadres1_a_b_c_d1.status);
	
			val reloadres1_a_b_c_d1_e1 = sutReload.get(PATH_a_b_c_d1_e1);
			Assert.assertEquals(NodeOverrideStatus.NOT_OVERRIDEN, reloadres1_a_b_c_d1_e1.status);

		}
		
		
		// -- Step 2 --
		NodeData data1_a_b_c_d1 = createDirData(d1, ImmutableSet.of());
		sut.put(PATH_a_b_c_d1, data1_a_b_c_d1);
		
		{
			// assert in-memory
			val res2_a_b_c_d1 = sut.get(PATH_a_b_c_d1);
			Assert.assertEquals(NodeOverrideStatus.UPDATED, res2_a_b_c_d1.status);

			// assert using reload
			val sutReload2 = reloader.get();
			
			val reloadres2_a_b_c = sutReload2.get(PATH_a_b_c);
			Assert.assertEquals(NodeOverrideStatus.UPDATED, reloadres2_a_b_c.status);
	
			val reloadres2_a_b_c_d1_e1 = sutReload2.get(PATH_a_b_c_d1_e1);
			Assert.assertEquals(NodeOverrideStatus.NOT_OVERRIDEN, reloadres2_a_b_c_d1_e1.status);
			
			val reloadres2_a_b_c_d1 = sutReload2.get(PATH_a_b_c_d1);
			Assert.assertEquals(NodeOverrideStatus.UPDATED, reloadres2_a_b_c_d1.status);

		}
		
		// -- Step 3 --
		sut.remove(PATH_a_b_c_d1);
		
		{
			// assert in-memory
			val res3_a_b_c_d1 = sut.get(PATH_a_b_c_d1);
			Assert.assertEquals(NodeOverrideStatus.DELETED, res3_a_b_c_d1.status);
	
			val res3_a_b_c_d1_e1 = sut.get(PATH_a_b_c_d1_e1);
			Assert.assertEquals(NodeOverrideStatus.DELETED, res3_a_b_c_d1_e1.status);
		
			// assert using reload
			val sutReload = reloader.get();
			
			val reloadres3_a_b_c = sutReload.get(PATH_a_b_c);
			Assert.assertEquals(NodeOverrideStatus.UPDATED, reloadres3_a_b_c.status);
			
			val reloadres3_a_b_c_d1 = sutReload.get(PATH_a_b_c_d1);
			Assert.assertEquals(NodeOverrideStatus.DELETED, reloadres3_a_b_c_d1.status);
	
			val reloadres3_a_b_c_d1_e1 = sutReload.get(PATH_a_b_c_d1_e1);
			Assert.assertEquals(NodeOverrideStatus.DELETED, reloadres3_a_b_c_d1_e1.status);
			
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

	private static AppendOnlyBlobStorage_PartialNodeDataByPath reloadFromFile(String baseFilename) {
		val res = new AppendOnlyBlobStorage_PartialNodeDataByPath(
				blobStorage, "test1", attrIndexes, nodeNameEncoder);
		res.initReload();
		return res;
	}

}
