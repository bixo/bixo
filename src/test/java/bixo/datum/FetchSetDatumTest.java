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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.scaleunlimited.cascading.PartitioningKey;
import com.scaleunlimited.cascading.Payload;

import cascading.tuple.Tuple;



public class FetchSetDatumTest {

    private static List<ScoredUrlDatum> makeUrls(int count, String payloadFieldName) {
        List<ScoredUrlDatum> results = new LinkedList<ScoredUrlDatum>();

       Payload payload = new Payload();

        for (int i = 0; i < count; i++) {
            ScoredUrlDatum url = new ScoredUrlDatum("http://domain.com/page-" + i, "key", UrlStatus.UNFETCHED, 1.0);
            if (payloadFieldName != null) {
                payload.put(payloadFieldName, "value-" + i);
                url.setPayload(payload);
            }

            results.add(url);
        }
        
        return results;
    }
        
    @Test
    public void testRoundTrip() {
        List<ScoredUrlDatum> urls = makeUrls(2, "meta1");
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        FetchSetDatum pfd1 = new FetchSetDatum(urls, fetchTime, 0, groupingKey.getValue(), groupingKey.getRef());
        pfd1.setLastList(true);
        
        Tuple t = pfd1.getTuple();
        
        FetchSetDatum pfd2 = new FetchSetDatum(t);
        
        Assert.assertEquals(pfd1, pfd2);
    }
    
    @Test
    public void testSerializable() throws Exception {
        List<ScoredUrlDatum> urls = makeUrls(2, "meta1");
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        FetchSetDatum pfd1 = new FetchSetDatum(urls, fetchTime, 1000, groupingKey.getValue(), groupingKey.getRef());

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(byteArray);
        oos.writeObject(pfd1);
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray.toByteArray()));
        FetchSetDatum pfd2 = (FetchSetDatum)ois.readObject();
 
        Assert.assertEquals(pfd1, pfd2);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testGroupBy() throws Exception {
        List<ScoredUrlDatum> urls = makeUrls(2, null);
        long fetchTime = System.currentTimeMillis();
        
        PartitioningKey groupingKey1 = new PartitioningKey("key1", 1);
        FetchSetDatum pfd1 = new FetchSetDatum(urls, fetchTime, 0, groupingKey1.getValue(), groupingKey1.getRef());
        pfd1.setLastList(true);
        Comparable c1 = pfd1.getGroupingKey();
        
        PartitioningKey groupingKey2 = new PartitioningKey("key2", 1);
        FetchSetDatum pfd2 = new FetchSetDatum(urls, fetchTime, 0, groupingKey2.getValue(), groupingKey2.getRef());
        pfd2.setLastList(true);
        Comparable c2 = pfd2.getGroupingKey();
        
        Assert.assertEquals(0, c1.compareTo(c2));
    }
}
