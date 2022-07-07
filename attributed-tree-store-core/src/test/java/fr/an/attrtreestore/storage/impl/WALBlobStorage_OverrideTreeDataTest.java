package fr.an.attrtreestore.storage.impl;

import java.io.File;
import java.util.Collections;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.FileBlobStorage;

import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.TstMkdirDirUtils;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.api.override.OverrideNodeStatus;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.AttrInfoIndexes;
import fr.an.attrtreestore.storage.api.TreeDataTstGenerator;
import lombok.val;

public class WALBlobStorage_OverrideTreeDataTest {

	private static final File baseDir = TstMkdirDirUtils.initMkdir("target/test-data/PartialNodeDataByPathTest");
	private static final BlobStorage blobStorage = new FileBlobStorage("test-data", baseDir);
	private static final AttrInfoIndexes attrIndexes; 
	private static final NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault(); 
	private static final AttrDataEncoderHelper attrDataEncoderHelper;
	
	TreeDataTstGenerator gen = new TreeDataTstGenerator();

	private static final NodeNamesPath PATH_a_b_c = nodeNameEncoder.encodePath("a/b/c");
	private static final NodeNamesPath PATH_a_b_c_d1 = nodeNameEncoder.encodePath("a/b/c/d1");
	private static final NodeNamesPath PATH_a_b_c_d1_e1 = nodeNameEncoder.encodePath("a/b/c/d1/e1");
	private static final NodeName c = nodeNameEncoder.encode("c");
	private static final NodeName d1 = nodeNameEncoder.encode("d1");
	private static final NodeName d2 = nodeNameEncoder.encode("d2");
	
	static {
		baseDir.mkdirs();
		attrIndexes = new AttrInfoIndexes(Collections.emptyList());
		attrDataEncoderHelper = new AttrDataEncoderHelper(attrIndexes, nodeNameEncoder);
	}
			
	@Test
	public void test_put_get_remove() {
		val sut = new WALBlobStorage_OverrideTreeData(blobStorage, "test1", attrDataEncoderHelper);
		sut.initCreateEmpty();
		
		Supplier<WALBlobStorage_OverrideTreeData> reloader = () -> reloadFromFile("test1");
		
		// -- Step 1 --
		NodeData data1_a_b_c = gen.createDirData(c, ImmutableSet.of(d1, d2));
		sut.put(PATH_a_b_c, data1_a_b_c);

		{
			// assert in-memory
			val res1_a_b_c = sut.getOverride(PATH_a_b_c);
			Assert.assertEquals(OverrideNodeStatus.UPDATED, res1_a_b_c.status);
	
			val res1_a_b_c_d1 = sut.getOverride(PATH_a_b_c_d1);
			Assert.assertEquals(OverrideNodeStatus.NOT_OVERRIDEN, res1_a_b_c_d1.status);
	
			val res1_a_b_c_d1_e1 = sut.getOverride(PATH_a_b_c_d1_e1);
			Assert.assertEquals(OverrideNodeStatus.NOT_OVERRIDEN, res1_a_b_c_d1_e1.status);

			// assert using reload
			val sutReload = reloader.get();

			val reloadres1_a_b_c = sutReload.getOverride(PATH_a_b_c);
			Assert.assertEquals(OverrideNodeStatus.UPDATED, reloadres1_a_b_c.status);
	
			val reloadres1_a_b_c_d1 = sutReload.getOverride(PATH_a_b_c_d1);
			Assert.assertEquals(OverrideNodeStatus.NOT_OVERRIDEN, reloadres1_a_b_c_d1.status);
	
			val reloadres1_a_b_c_d1_e1 = sutReload.getOverride(PATH_a_b_c_d1_e1);
			Assert.assertEquals(OverrideNodeStatus.NOT_OVERRIDEN, reloadres1_a_b_c_d1_e1.status);

		}
		
		
		// -- Step 2 --
		NodeData data1_a_b_c_d1 = gen.createDirData(d1, ImmutableSet.of());
		sut.put(PATH_a_b_c_d1, data1_a_b_c_d1);
		
		{
			// assert in-memory
			val res2_a_b_c_d1 = sut.getOverride(PATH_a_b_c_d1);
			Assert.assertEquals(OverrideNodeStatus.UPDATED, res2_a_b_c_d1.status);

			// assert using reload
			val sutReload2 = reloader.get();
			
			val reloadres2_a_b_c = sutReload2.getOverride(PATH_a_b_c);
			Assert.assertEquals(OverrideNodeStatus.UPDATED, reloadres2_a_b_c.status);
	
			val reloadres2_a_b_c_d1_e1 = sutReload2.getOverride(PATH_a_b_c_d1_e1);
			Assert.assertEquals(OverrideNodeStatus.NOT_OVERRIDEN, reloadres2_a_b_c_d1_e1.status);
			
			val reloadres2_a_b_c_d1 = sutReload2.getOverride(PATH_a_b_c_d1);
			Assert.assertEquals(OverrideNodeStatus.UPDATED, reloadres2_a_b_c_d1.status);

		}
		
		// -- Step 3 --
		sut.remove(PATH_a_b_c_d1);
		
		{
			// assert in-memory
			val res3_a_b_c_d1 = sut.getOverride(PATH_a_b_c_d1);
			Assert.assertEquals(OverrideNodeStatus.DELETED, res3_a_b_c_d1.status);
	
			val res3_a_b_c_d1_e1 = sut.getOverride(PATH_a_b_c_d1_e1);
			Assert.assertEquals(OverrideNodeStatus.DELETED, res3_a_b_c_d1_e1.status);
		
			// assert using reload
			val sutReload = reloader.get();
			
			val reloadres3_a_b_c = sutReload.getOverride(PATH_a_b_c);
			Assert.assertEquals(OverrideNodeStatus.UPDATED, reloadres3_a_b_c.status);
			
			val reloadres3_a_b_c_d1 = sutReload.getOverride(PATH_a_b_c_d1);
			Assert.assertEquals(OverrideNodeStatus.DELETED, reloadres3_a_b_c_d1.status);
	
			val reloadres3_a_b_c_d1_e1 = sutReload.getOverride(PATH_a_b_c_d1_e1);
			Assert.assertEquals(OverrideNodeStatus.DELETED, reloadres3_a_b_c_d1_e1.status);
			
		}
		
	}
	
	private static WALBlobStorage_OverrideTreeData reloadFromFile(String baseFilename) {
		val res = new WALBlobStorage_OverrideTreeData(blobStorage, "test1", attrDataEncoderHelper);
		res.initReload();
		return res;
	}

}
