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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;



public class CrawlDirUtilsTest {
    
    BasePlatform _platform;
    BasePath _outputPath;
    
    @Before
    public void setUp() throws Exception {
        _platform = new BixoPlatform(CrawlDirUtilsTest.class, Platform.Local);
        _outputPath = _platform.makePath("./build/CrawlDirUtilsTest");
        _outputPath.mkdirs();
    }
    
    @After
    public void tearDown() throws Exception {
        _outputPath.delete(true);
    }

    @Test
    public void testMakeLoopDir() throws Exception {
        BasePath loopPath = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 3);
        Assert.assertTrue(loopPath.toString().startsWith(_outputPath.toString() + "/3-"));
        Assert.assertTrue(loopPath.exists());
    }
    
    @Test
    public void testFindLatestLoopDir() throws Exception {
        CrawlDirUtils.makeLoopDir(_platform, _outputPath, 0);
        CrawlDirUtils.makeLoopDir(_platform, _outputPath, 1);
        CrawlDirUtils.makeLoopDir(_platform, _outputPath, 3);
        CrawlDirUtils.makeLoopDir(_platform, _outputPath, 7);
        BasePath expectedPath =
            CrawlDirUtils.makeLoopDir(_platform, _outputPath, 11);
        Assert.assertEquals(expectedPath.toString(),
                            CrawlDirUtils.findLatestLoopDir(_platform, _outputPath).toString());
    }
    
    @Test
    public void testFindNextLoopDir() throws Exception {
        CrawlDirUtils.makeLoopDir(_platform, _outputPath, 0);
        BasePath path1 =
            CrawlDirUtils.makeLoopDir(_platform, _outputPath, 1);
        BasePath path3 =
            CrawlDirUtils.makeLoopDir(_platform, _outputPath, 3);
        BasePath path7 =
            CrawlDirUtils.makeLoopDir(_platform, _outputPath, 7);
        CrawlDirUtils.makeLoopDir(_platform, _outputPath, 11);
        Assert.assertEquals(path1.toString(),
                            CrawlDirUtils.findNextLoopDir(_platform, _outputPath, 0).toString());
        Assert.assertEquals(path3.toString(),
                            CrawlDirUtils.findNextLoopDir(_platform, _outputPath, 1).toString());
        Assert.assertEquals(path7.toString(),
                            CrawlDirUtils.findNextLoopDir(_platform, _outputPath, 4).toString());
    }

    @Test
    public void testExtractLoopNumber() throws Exception {
        BasePath path0 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 0);
        BasePath path1 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 1);
        BasePath path3 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 3);
        BasePath path7 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 7);
        BasePath path11 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 11);
        Assert.assertEquals(0, CrawlDirUtils.extractLoopNumber(path0));
        Assert.assertEquals(1, CrawlDirUtils.extractLoopNumber(path1));
        Assert.assertEquals(3, CrawlDirUtils.extractLoopNumber(path3));
        Assert.assertEquals(7, CrawlDirUtils.extractLoopNumber(path7));
        Assert.assertEquals(11, CrawlDirUtils.extractLoopNumber(path11));
    }
 
    @Test
    public void testFindAllSubdirs() throws Exception {
        // Make a loop dir with a subdir
        String subdirName = "bogus";
        BasePath path0 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 0);
        BasePath subdirPath0 = _platform.makePath(path0, subdirName);
        subdirPath0.mkdirs();
        
        // And another one without the subdir
        BasePath path1 = CrawlDirUtils.makeLoopDir(_platform, _outputPath, 1);

        BasePath[] allSubdirPathsArr = CrawlDirUtils.findAllSubdirs(_platform, _outputPath, subdirName);
        Assert.assertEquals(1, allSubdirPathsArr.length);

        // Now add a subdir to path1 as well
        BasePath subdirPath1 = _platform.makePath(path1, subdirName);
        subdirPath1.mkdirs();
        BasePath[] strictSubdirPathsArr = CrawlDirUtils.findAllSubdirs(_platform, _outputPath, subdirName);
        Assert.assertEquals(2, strictSubdirPathsArr.length);
    }
}
