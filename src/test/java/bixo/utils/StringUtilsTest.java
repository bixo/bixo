/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
