/*
 * Copyright 2009-2012 Scale Unlimited
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
package bixo.hadoop;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.bixolabs.cascading.HadoopUtils;



@SuppressWarnings("deprecation")
public class HadoopUtilsTest {

	@Test
	public void testStackSizeSetup() throws IOException, InterruptedException {
		JobConf conf = HadoopUtils.getDefaultJobConf(512);
		Assert.assertTrue(conf.get("mapred.child.java.opts").contains("-Xss512k"));
	}
	
	@Test
	public void testDebugLevel() throws IOException, InterruptedException {
		JobConf conf = HadoopUtils.getDefaultJobConf(512);
		Properties props = HadoopUtils.getDefaultProperties(HadoopUtilsTest.class, true, conf);
		Assert.assertTrue(props.getProperty("log4j.logger").contains("bixo=TRACE"));
	}
}
