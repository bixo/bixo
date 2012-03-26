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
package bixo.utils;


import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bixolabs.cascading.HadoopUtils;


public class CrawlDirUtilsTest {
    
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
 
    @Test
    public void testFindAllSubdirs() throws IOException {
        // Make a loop dir with a subdir
        String subdirName = "bogus";
        Path path0 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 0);
        Path subdirPath0 = new Path(path0, subdirName);
        _fileSystem.mkdirs(subdirPath0);
        
        // And another one without the subdir
        Path path1 = CrawlDirUtils.makeLoopDir(_fileSystem, _outputPath, 1);

        Path[] allSubdirPathsArr = CrawlDirUtils.findAllSubdirs(_fileSystem, _outputPath, subdirName);
        Assert.assertEquals(1, allSubdirPathsArr.length);

        // Now add a subdir to path1 as well
        Path subdirPath1 = new Path(path1, subdirName);
        _fileSystem.mkdirs(subdirPath1);
        Path[] strictSubdirPathsArr = CrawlDirUtils.findAllSubdirs(_fileSystem, _outputPath, subdirName);
        Assert.assertEquals(2, strictSubdirPathsArr.length);
    }
}
