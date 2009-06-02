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
}
