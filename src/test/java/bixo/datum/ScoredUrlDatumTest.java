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
package bixo.datum;

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;
import bixo.utils.DiskQueue;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.PartitioningKey;



public class ScoredUrlDatumTest {

    @Test
    public void testSerializable() {
        DiskQueue<ScoredUrlDatum> queue = new DiskQueue<ScoredUrlDatum>(1);
        
        ScoredUrlDatum datum = new ScoredUrlDatum("http://domain.com");
        try {
            Assert.assertTrue(queue.offer(datum));
            Assert.assertTrue(queue.offer(datum));

            Assert.assertEquals("http://domain.com", queue.poll().getUrl());
            Assert.assertEquals("http://domain.com", queue.poll().getUrl());
            Assert.assertNull(queue.poll());
        } catch (Exception e) {
            Assert.fail("ScoredUrlDatum must be serializable");
        }
    }
    
    @Test
    public void testCascadingHadoopSerialization() throws Exception {
        testSerialization(Platform.Hadoop);
    }
    
    @Test
    public void testCascadingLocalSerialization() throws Exception {
        testSerialization(Platform.Local);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void testSerialization(Platform  platformMode) throws Exception {
        List<ScoredUrlDatum> urls = new LinkedList<ScoredUrlDatum>();
        ScoredUrlDatum url = new ScoredUrlDatum("http://domain.com/page-1", "key", UrlStatus.UNFETCHED, 1.0);
        urls.add(url);
        
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        FetchSetDatum pfd = new FetchSetDatum(urls, fetchTime, 1000, groupingKey.getValue(), groupingKey.getRef());
        
        BixoPlatform platform = new BixoPlatform(ScoredUrlDatumTest.class, platformMode);
        BasePath path = platform.makePath("build/test/ScoredUrlDatumTest/testCascadingSerialization/in");
        Tap in = platform.makeTap(platform.makeBinaryScheme(FetchSetDatum.FIELDS), path, SinkMode.REPLACE);
        TupleEntryCollector write = in.openForWrite(platform.makeFlowProcess());
        write.add(pfd.getTuple());
        write.close();
    }
}
