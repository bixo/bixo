package bixo;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

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
