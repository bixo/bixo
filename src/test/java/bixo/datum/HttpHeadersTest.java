package bixo.datum;

import org.junit.Assert;
import org.junit.Test;


public class HttpHeadersTest {

    @Test
    public void testMultiValues() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("key", "value1");
        headers.add("key", "value2");
        
        Assert.assertEquals("value1", headers.getFirst("key"));
        Assert.assertEquals(2, headers.getAll("key").size());
    }
    
    @Test
    public void testEncodeDecode() {
        HttpHeaders headers = new HttpHeaders();
        String key1 = "key\twith\ttabs";
        String value1 = "value1";
        headers.add(key1, value1);
        
        String encoded = headers.toString();
        
        HttpHeaders newHeaders = new HttpHeaders(encoded);
        Assert.assertEquals(1, newHeaders.getNames().size());
        Assert.assertEquals(value1, newHeaders.getFirst(key1));
        
        String key2 = "key\n\r\fwith lots of funky chars";
        String value2 = "value2";
        
        headers.add(key2, value2);
        encoded = headers.toString();
        
        newHeaders = new HttpHeaders(encoded);
        Assert.assertEquals(2, newHeaders.getNames().size());
        Assert.assertEquals(value1, newHeaders.getFirst(key1));
        Assert.assertEquals(value2, newHeaders.getFirst(key2));
    }
}
