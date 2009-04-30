/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.utils;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

import bixo.utils.HadoopConfigured;

import cascading.ClusterTestCase;
import cascading.flow.MultiMapReducePlanner;

public class HadoopConfiguredTest extends ClusterTestCase {

    public HadoopConfiguredTest() {
        super("hadoopConfigTest", true);
    }

    @Test
    public void testFileSystem() throws Exception {
        HadoopConfigured configured = new HadoopConfigured();
        FileSystem fileSystem = configured.getFileSystem();
        Assert.assertTrue(fileSystem instanceof LocalFileSystem);
        Assert.assertTrue(configured.getFileSystem(new URI("file://tmp")) instanceof LocalFileSystem);

        JobConf conf = MultiMapReducePlanner.getJobConf(getProperties());
        URI defaultUri = FileSystem.getDefaultUri(conf);

        Assert.assertTrue(configured.getFileSystem(defaultUri) instanceof DistributedFileSystem);

        Assert.assertTrue(configured.getFileSystem("/somePath") instanceof LocalFileSystem);
        Assert.assertTrue(configured.getFileSystem("file:///somePath") instanceof LocalFileSystem);
        Assert.assertTrue(configured.getFileSystem(defaultUri.toASCIIString() + "/something") instanceof DistributedFileSystem);
    }

}
