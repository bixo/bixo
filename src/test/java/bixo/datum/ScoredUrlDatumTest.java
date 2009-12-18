package bixo.datum;

import org.junit.Assert;
import org.junit.Test;

import bixo.utils.DiskQueue;


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
}
