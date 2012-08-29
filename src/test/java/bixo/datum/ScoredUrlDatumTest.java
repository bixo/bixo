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
package bixo.datum;

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import bixo.utils.DiskQueue;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.PartitioningKey;


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
    public void testCascadingSerialization() throws Exception {
        List<ScoredUrlDatum> urls = new LinkedList<ScoredUrlDatum>();
        ScoredUrlDatum url = new ScoredUrlDatum("http://domain.com/page-1", "key", UrlStatus.UNFETCHED, 1.0);
        urls.add(url);
        
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        FetchSetDatum pfd = new FetchSetDatum(urls, fetchTime, 1000, groupingKey.getValue(), groupingKey.getRef());
        
        Lfs in = new Lfs(new SequenceFile(FetchSetDatum.FIELDS), "build/test/ScoredUrlDatumTest/testCascadingSerialization/in", true);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        write.add(pfd.getTuple());
        write.close();
    }
}
