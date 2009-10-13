package bixo.utils;

import junit.framework.Assert;

import org.junit.Test;


public class StringUtilsTest {

	@Test
	public void testSplitOnChar() {
		final String strings[] = {
				"a", "1", "a",
				"\ta", "2", "", "a",
				"a\t", "2", "a", "",
				"a\tb", "2", "a", "b",
				"\ta\tb", "3", "", "a", "b",
				"a\tb\t", "3", "a", "b", "",
				"\ta\tb\t", "4", "", "a", "b", "",
				"a\t\tb", "3", "a", "", "b",
				"a\t\t", "3", "a", "", ""
			};
		
		int stringIndex = 0;
		while (stringIndex < strings.length) {
			String[] splits = StringUtils.splitOnChar(strings[stringIndex++], '\t');
			
			Assert.assertEquals(Integer.parseInt(strings[stringIndex++]), splits.length);
			
			for (String split : splits) {
				Assert.assertEquals(strings[stringIndex++], split);
			}
		}
		

	}
}
