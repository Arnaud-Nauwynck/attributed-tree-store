package fr.an.attrtreestore.storage;

import org.junit.Assert;
import org.junit.Test;


public class AttrDataEncoderHelperTest {

	@Test
	public void testCommon() {
		Assert.assertEquals(3, AttrDataEncoderHelper.commonStringLen("abcX", "abcY"));
		Assert.assertEquals(3, AttrDataEncoderHelper.commonStringLen("abcX", "abc"));
		Assert.assertEquals(3, AttrDataEncoderHelper.commonStringLen("abc", "abcY"));
		Assert.assertEquals(3, AttrDataEncoderHelper.commonStringLen("abc", "abc"));
		Assert.assertEquals(0, AttrDataEncoderHelper.commonStringLen("X", "Y"));
		Assert.assertEquals(0, AttrDataEncoderHelper.commonStringLen("X", ""));
		Assert.assertEquals(0, AttrDataEncoderHelper.commonStringLen("", "Y"));
	}
}
