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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.scaleunlimited.cascading.Payload;


import cascading.tuple.Tuple;

public class FetchedDatumTest {

    @Test
    public void testFlatteningHeaders() {
        String url = "http://domain.com";
        
        HttpHeaders headers = new HttpHeaders();
        String key1 = "key1\twith\ttab";
        headers.add(key1, "value");
        
        String value2 = "value2\\with\\backslashes";
        headers.add("key2", value2);
        
        String value3 = "value3\nwith\nreturns";
        headers.add("key3", value3);
        FetchedDatum datum = new FetchedDatum(url, url, 0, headers, new ContentBytes(), "", 0);
        
        Tuple tuple = datum.getTuple();
        
        FetchedDatum newDatum = new FetchedDatum(tuple);
        
        HttpHeaders newHeaders = newDatum.getHeaders();
        assertEquals("value", newHeaders.getFirst(key1));
        assertEquals(value2, newHeaders.getFirst("key2"));
        assertEquals(value3, newHeaders.getFirst("key3"));
    }
    
    @Test
    public void testCreatingFromTextLine() {
        // TODO KKr - create an Lfs that writes out a FetchedDatum as a TextLine, and then
        // reads it back in.
    }
    
    @Test
    public void testToString() {
        String url = "http://domain.com";
        
        HttpHeaders headers = new HttpHeaders();
        headers.add("status code", "200");
        FetchedDatum datum = new FetchedDatum(url, url, 0, headers, new ContentBytes(), "", 0);

        String result = datum.toString();
        assertFalse(result.contains("FetchedDatum@"));
    }
    
    @Test
    public void testCreatingFromParams() throws Exception {
        FetchedDatum datum = new FetchedDatum("baseUrl", "fetchedUrl",
                        0,
                        new HttpHeaders(),
                        new ContentBytes(),
                        "text/html",
                        0);
        assertEquals("baseUrl", datum.getUrl());
        assertEquals(0, datum.getFetchTime());
    }
    
    @Test
    public void testCreatingFromScoredUrlDatum() throws Exception {
        ScoredUrlDatum sud = new ScoredUrlDatum("url", "groupKey", UrlStatus.UNFETCHED);
        Payload payload = new Payload();
        payload.put("key", "value");
        sud.setPayload(payload);
        
        FetchedDatum fd = new FetchedDatum(sud);
        assertEquals("value", fd.getPayload().get("key"));
    }
    
}
