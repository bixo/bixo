package bixo.datum;

import org.junit.Assert;
import org.junit.Test;

import cascading.tuple.Fields;
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
        FetchedDatum datum = new FetchedDatum(FetchStatusCode.UNFETCHED, url, url, 0, headers, null, null, 0, null);
        
        Tuple tuple = datum.toTuple();
        
        FetchedDatum newDatum = new FetchedDatum(tuple, new Fields());
        
        HttpHeaders newHeaders = newDatum.getHeaders();
        Assert.assertEquals("value", newHeaders.getFirst(key1));
        Assert.assertEquals(value2, newHeaders.getFirst("key2"));
        Assert.assertEquals(value3, newHeaders.getFirst("key3"));
    }
}
