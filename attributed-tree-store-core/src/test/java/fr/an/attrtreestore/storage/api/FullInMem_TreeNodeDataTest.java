package fr.an.attrtreestore.storage.api;

import java.io.File;
import java.util.Collections;

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
import fr.an.attrtreestore.storage.impl.IndexedBlobStorage_TreeNodeDataEncoder;
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
		sut.put_strictNoCreateParent(PATH_a, createDirData(a, ImmutableSet.of(b)));
		sut.put_strictNoCreateParent(PATH_a_b, createDirData(b, ImmutableSet.of(c)));
		sut.put_strictNoCreateParent(PATH_a_b_c, createDirData(c, ImmutableSet.of(d1, d2, d3)));
		sut.put_strictNoCreateParent(PATH_a_b_c_d1, createDirData(d1, ImmutableSet.of(e1)));
		sut.put_strictNoCreateParent(PATH_a_b_c_d1_e1, createDirData(e1, ImmutableSet.of()));
		sut.put_strictNoCreateParent(PATH_a_b_c_d2, createDirData(d2, ImmutableSet.of()));
		sut.put_strictNoCreateParent(PATH_a_b_c_d3, createDirData(d3, ImmutableSet.of()));
		
		// write to file
		sut.recursiveWriteFull(blobStorage, "test-full-treedata1", indexedEncoder);
		
		// TODO re-read full file... and navigate + compare

		// TODO re-read only file with root .. and navigate with cache resolver + compare

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
}
