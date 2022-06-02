package fr.an.attrtreestore.storage.api;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.FullInMem_TreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoder;
import lombok.val;

public class TreeTstObj {

	static final NodeNameEncoder nodeNameEncoder = DefaultNodeNameEncoder.createDefault();
	
	TreeDataTstGenerator gen = new TreeDataTstGenerator();
	

	@Test
	public void test_put_recursiveWriteFull() {
		val tree_a_b_c_d = new TreeTstObj();
		Assert.assertNotNull(tree_a_b_c_d.treeData);
	}

	public static final NodeName root = nodeNameEncoder.encode("");
	public static final NodeName a = nodeNameEncoder.encode("a");
	public static final NodeName b = nodeNameEncoder.encode("b");
	public static final NodeName c = nodeNameEncoder.encode("c");
	public static final NodeName d1 = nodeNameEncoder.encode("d1");
	public static final NodeName e1 = nodeNameEncoder.encode("e1");
	public static final NodeName d2 = nodeNameEncoder.encode("d2");
	public static final NodeName d3 = nodeNameEncoder.encode("d3");
	
	public static final NodeNamesPath PATH_a = NodeNamesPath.of(a);
	public static final NodeNamesPath PATH_a_b = NodeNamesPath.of(a, b);
	public static final NodeNamesPath PATH_a_b_c = NodeNamesPath.of(a, b, c);
	public static final NodeNamesPath PATH_a_b_c_d1 = NodeNamesPath.of(a, b, c, d1);
	public static final NodeNamesPath PATH_a_b_c_d1_e1 = NodeNamesPath.of(a, b, c, d1, e1);
	public static final NodeNamesPath PATH_a_b_c_d2 = NodeNamesPath.of(a, b, c, d2);
	public static final NodeNamesPath PATH_a_b_c_d3 = NodeNamesPath.of(a, b, c, d3);
	
	public final FullInMem_TreeData treeData = new FullInMem_TreeData();
	
	public final NodeData data_a;
	public final NodeData data_a_b;
	public final NodeData data_a_b_c;
	public final NodeData data_a_b_c_d1;
	public final NodeData data_a_b_c_d1_e1;
	public final NodeData data_a_b_c_d2;
	public final NodeData data_a_b_c_d3;
	
	public TreeTstObj() {
		// fill recursive some data..
		treeData.put_root(gen.createDirData(root, ImmutableSet.of(a)));
		
		data_a = gen.createDirData(a, ImmutableSet.of(b));
		treeData.put_strictNoCreateParent(PATH_a, data_a);

		data_a_b = gen.createDirData(b, ImmutableSet.of(c));
		treeData.put_strictNoCreateParent(PATH_a_b, data_a_b);
		
		data_a_b_c = gen.createDirData(c, ImmutableSet.of(d1, d2, d3));
		treeData.put_strictNoCreateParent(PATH_a_b_c, data_a_b_c);
		
		data_a_b_c_d1 = gen.createDirData(d1, ImmutableSet.of(e1));
		treeData.put_strictNoCreateParent(PATH_a_b_c_d1, data_a_b_c_d1);
		
		data_a_b_c_d1_e1 = gen.createDirData(e1, ImmutableSet.of());
		treeData.put_strictNoCreateParent(PATH_a_b_c_d1_e1, data_a_b_c_d1_e1);
		
		data_a_b_c_d2 = gen.createDirData(d2, ImmutableSet.of());
		treeData.put_strictNoCreateParent(PATH_a_b_c_d2, data_a_b_c_d2);
		
		data_a_b_c_d3 = gen.createDirData(d3, ImmutableSet.of());
		treeData.put_strictNoCreateParent(PATH_a_b_c_d3, data_a_b_c_d3);
	}		

}
