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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import cascading.tuple.Tuple;


public class HttpHeadersTest {

    @Test
    public void testMultiValues() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("key", "value1");
        headers.add("key", "value2");
        
        assertEquals("value1", headers.getFirst("key"));
        List<String> values = headers.getAll("key");
        assertEquals(2, values.size());
        
        Collections.sort(values);
        assertEquals("value1", values.get(0));
        assertEquals("value2", values.get(1));
    }
    
    @Test
    public void testEncodeDecode() {
        HttpHeaders headers = new HttpHeaders();
        String key1 = "key\twith\ttabs";
        String value1 = "value1";
        headers.add(key1, value1);
        
        Tuple t = headers.toTuple();
        HttpHeaders newHeaders = new HttpHeaders(t);
        assertEquals(1, newHeaders.getNames().size());
        assertEquals(value1, newHeaders.getFirst(key1));
        
        String key2 = "key\n\r\fwith lots of funky chars";
        String value2 = "value2";
        headers.add(key2, value2);
        
        t = headers.toTuple();
        newHeaders = new HttpHeaders(t);
        assertEquals(2, newHeaders.getNames().size());
        assertEquals(value1, newHeaders.getFirst(key1));
        assertEquals(value2, newHeaders.getFirst(key2));
        
        String key3 = "date";
        String value3 = "Wed, 11 May 2011 18:26:45 GMT";
        headers.add(key3, value3);
        
        t = headers.toTuple();
        newHeaders = new HttpHeaders(t);
        assertEquals(value3, newHeaders.getFirst(key3));
    }
    
    @Test
    public void testToString() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.add("key1", "value1");
        headers.add("key1", "value2");
        headers.add("key2", "value3,value4");

        String result = headers.toString();
        System.out.println(result);
        assertTrue(result.contains("key1="));
        assertTrue(result.contains("key2="));
    }
    
    @Test
    public void testSerialization() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.add("key1", "value1");
        headers.add("key1", "value2");
        headers.add("key2", "value3");

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteStream);
        headers.write(out);
        
        HttpHeaders newHeaders = new HttpHeaders();
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        newHeaders.readFields(in);
        
        assertEquals("value1", newHeaders.getFirst("key1"));
        List<String> values = newHeaders.getAll("key1");
        assertEquals(2, values.size());
        
        Collections.sort(values);
        assertEquals("value1", values.get(0));
        assertEquals("value2", values.get(1));

        values = newHeaders.getAll("key2");
        assertEquals(1, values.size());
        
        assertEquals(2, newHeaders.getNames().size());
    }
}
