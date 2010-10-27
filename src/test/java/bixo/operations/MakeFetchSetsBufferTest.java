package bixo.operations;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class MakeFetchSetsBufferTest {

    @Test
    public void testRandomMovingRequestTime() throws Exception {
        Random rand = new Random();
        
        // See how it works at very end of range.
        Assert.assertEquals(Long.MAX_VALUE, MakeFetchSetsBuffer.randRequestTime(rand, 100, Long.MAX_VALUE - 1));
        
        long curRequestTime = 0;
        
        for (int i = 0; i < 10000; i++) {
            long newRequestTime = MakeFetchSetsBuffer.randRequestTime(rand, 1000, curRequestTime);
            Assert.assertTrue(newRequestTime > curRequestTime);
            curRequestTime = newRequestTime;
        }
    }
}
