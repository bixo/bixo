package bixo.cascading;

import junit.framework.Assert;

import org.junit.Test;

import bixo.datum.Payload;

import cascading.tuple.Tuple;


public class PayloadTest {

    @Test
    public void testTupleRoundtrip() throws Exception {
        Payload p1 = new Payload();
        p1.put("key1", "value1");
        p1.put("key2", "value2");
        
        Tuple t = p1.toTuple();
        
        Payload p2 = new Payload(t);
        Assert.assertEquals(p1, p2);
    }
}
