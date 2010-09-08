package bixo.utils;


import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bixo.hadoop.HadoopUtils;

public class FsUtilsTest {
    
    JobConf _conf;
    FileSystem _fileSystem;
    Path _outputPath;

    @Before
    public void setUp() throws Exception {
        _conf = HadoopUtils.getDefaultJobConf();
        _outputPath = new Path("./build/FsUtilsTest");
        _fileSystem = _outputPath.getFileSystem(_conf);
        _fileSystem.mkdirs(_outputPath);
    }
    
    @After
    public void tearDown() throws Exception {
        _fileSystem.delete(_outputPath, true);
    }

    @Test
    public void testMakeLoopDir() throws IOException {
        Path loopPath = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 3);
        Assert.assertTrue(loopPath.toString().startsWith(_outputPath.toString() + "/3-"));
        Assert.assertTrue(_fileSystem.exists(loopPath));
    }
    
    @Test
    public void testFindLatestLoopDir() throws IOException {
        CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 0);
        CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 1);
        CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 3);
        CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 7);
        Path expectedPath =
            CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 11).makeQualified(_fileSystem);
        Assert.assertEquals(expectedPath.toString(),
                            CrawlDirUtils.findLatestLoopDir(_fileSystem, _outputPath).makeQualified(_fileSystem).toString());
    }
    
    @Test
    public void testFindNextLoopDir() throws IOException {
        CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 0);
        Path path1 =
            CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 1).makeQualified(_fileSystem);
        Path path3 =
            CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 3).makeQualified(_fileSystem);
        Path path7 =
            CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 7).makeQualified(_fileSystem);
        CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 11);
        Assert.assertEquals(path1.toString(),
                            CrawlDirUtils.findNextLoopDir(_fileSystem, _outputPath, 0).makeQualified(_fileSystem).toString());
        Assert.assertEquals(path3.toString(),
                            CrawlDirUtils.findNextLoopDir(_fileSystem, _outputPath, 1).makeQualified(_fileSystem).toString());
        Assert.assertEquals(path7.toString(),
                            CrawlDirUtils.findNextLoopDir(_fileSystem, _outputPath, 4).makeQualified(_fileSystem).toString());
    }

    @Test
    public void testExtractLoopNumber() throws IOException {
        Path path0 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 0);
        Path path1 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 1);
        Path path3 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 3);
        Path path7 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 7);
        Path path11 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 11);
        Assert.assertEquals(0, CrawlDirUtils.extractLoopNumber(path0));
        Assert.assertEquals(1, CrawlDirUtils.extractLoopNumber(path1));
        Assert.assertEquals(3, CrawlDirUtils.extractLoopNumber(path3));
        Assert.assertEquals(7, CrawlDirUtils.extractLoopNumber(path7));
        Assert.assertEquals(11, CrawlDirUtils.extractLoopNumber(path11));
    }
    
}
