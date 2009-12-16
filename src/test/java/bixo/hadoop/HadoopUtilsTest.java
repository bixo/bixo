package bixo.hadoop;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.hadoop.HadoopUtils;


public class HadoopUtilsTest {

	@Test
	public void testStackSizeSetup() throws IOException {
		JobConf conf = HadoopUtils.getDefaultJobConf(512);
		Assert.assertTrue(conf.get("mapred.child.java.opts").contains("-Xss512k"));
	}
	
	@Test
	public void testDebugLevel() throws IOException {
		JobConf conf = HadoopUtils.getDefaultJobConf(512);
		Properties props = HadoopUtils.getDefaultProperties(HadoopUtilsTest.class, true, conf);
		Assert.assertTrue(props.getProperty("log4j.logger").contains("bixo=TRACE"));
	}
}
