package bixo.cascading;

import junit.framework.Assert;

import org.junit.Test;

public class PartitioningKeyTest {

    @Test
    public void testValues() throws Exception {
        final int numReducers = 1;
        PartitioningKey key = new PartitioningKey("test", numReducers);
        
        for (int i = 0; i < 100; i++) {
            PartitioningKey otherKey = new PartitioningKey("test-" + i, numReducers);
            Assert.assertEquals(key.getValue(), otherKey.getValue());
        }
    }
}
