package bixo.datum;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import bixo.cascading.PartitioningKey;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


public class PreFetchedDatumTest {

    @SuppressWarnings("unchecked")
    private static List<ScoredUrlDatum> makeUrls(int count, String metadataFieldName) {
        List<ScoredUrlDatum> results = new LinkedList<ScoredUrlDatum>();

        Map<String, Comparable> metaData = new HashMap<String, Comparable>();

        for (int i = 0; i < count; i++) {
            metaData.put(metadataFieldName, "value-" + i);
            ScoredUrlDatum url = new ScoredUrlDatum("http://domain.com/page-" + i, 0, 0, UrlStatus.UNFETCHED, "key", 1.0, metaData);
            results.add(url);
        }
        
        return results;
    }
        
    @Test
    public void testRoundTrip() {
        List<ScoredUrlDatum> urls = makeUrls(2, "meta1");
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        PreFetchedDatum pfd1 = new PreFetchedDatum(urls, fetchTime, 0, groupingKey, true);
        
        Tuple t = pfd1.toTuple();
        
        PreFetchedDatum pfd2 = new PreFetchedDatum(t, new Fields("meta1"));
        
        Assert.assertEquals(pfd1, pfd2);
    }
    
    @Test
    public void testSerializable() throws Exception {
        List<ScoredUrlDatum> urls = makeUrls(2, "meta1");
        long fetchTime = System.currentTimeMillis();
        PartitioningKey groupingKey = new PartitioningKey("key", 1);
        PreFetchedDatum pfd1 = new PreFetchedDatum(urls, fetchTime, 1000, groupingKey, false);

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(byteArray);
        oos.writeObject(pfd1);
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray.toByteArray()));
        PreFetchedDatum pfd2 = (PreFetchedDatum)ois.readObject();
 
        Assert.assertEquals(pfd1, pfd2);

    }
}
