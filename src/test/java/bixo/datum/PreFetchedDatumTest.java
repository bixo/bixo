package bixo.datum;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import bixo.cascading.PartitioningKey;
import bixo.cascading.Payload;
import cascading.tuple.Tuple;


public class PreFetchedDatumTest {

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
        PreFetchedDatum pfd1 = new PreFetchedDatum(urls, fetchTime, 0, groupingKey.getValue(), groupingKey.getRef(), true);
        
        Tuple t = pfd1.getTuple();
        
        PreFetchedDatum pfd2 = new PreFetchedDatum(t);
        
        Assert.assertEquals(pfd1, pfd2);
    }
    
    @Test
    public void testSerializable() throws Exception {
        List<ScoredUrlDatum> urls = makeUrls(2, "meta1");
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        PreFetchedDatum pfd1 = new PreFetchedDatum(urls, fetchTime, 1000, groupingKey.getValue(), groupingKey.getRef(), false);

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(byteArray);
        oos.writeObject(pfd1);
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray.toByteArray()));
        PreFetchedDatum pfd2 = (PreFetchedDatum)ois.readObject();
 
        Assert.assertEquals(pfd1, pfd2);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testGroupBy() throws Exception {
        List<ScoredUrlDatum> urls = makeUrls(2, null);
        long fetchTime = System.currentTimeMillis();
        
        PartitioningKey groupingKey1 = new PartitioningKey("key1", 1);
        PreFetchedDatum pfd1 = new PreFetchedDatum(urls, fetchTime, 0, groupingKey1.getValue(), groupingKey1.getRef(), true);
        Comparable c1 = pfd1.getGroupingKey();
        
        PartitioningKey groupingKey2 = new PartitioningKey("key2", 1);
        PreFetchedDatum pfd2 = new PreFetchedDatum(urls, fetchTime, 0, groupingKey2.getValue(), groupingKey2.getRef(), true);
        Comparable c2 = pfd2.getGroupingKey();
        
        Assert.assertEquals(0, c1.compareTo(c2));
    }
}
