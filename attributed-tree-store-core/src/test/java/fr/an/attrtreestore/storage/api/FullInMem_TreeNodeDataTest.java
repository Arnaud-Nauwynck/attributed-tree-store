package fr.an.attrtreestore.storage.api;

import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class FullInMem_TreeNodeDataTest {
	
	@Test
	public void test_put_recursiveWriteFull() {
		val tree_a_b_c_d = new TreeTstObj();
		Assert.assertNotNull(tree_a_b_c_d.treeData);
	}

}
