package bixo.cascading;

import junit.framework.Assert;

import org.junit.Test;


public class PartitioningKeyTest {

    @Test
    public void testRoundTrip() {
        PartitioningKey key1 = new PartitioningKey("test", 2);
        
        String keyAsString = key1.toString();
        
        PartitioningKey key2 = new PartitioningKey(keyAsString);
        Assert.assertEquals(key1, key2);
    }
    
    @Test
    public void testMultipleDashes() {
        try {
            PartitioningKey key1 = new PartitioningKey("0.0.0.0-unset-0", 2);
            String keyAsString = key1.toString();
            PartitioningKey key2 = new PartitioningKey(keyAsString);
            Assert.assertEquals(key1, key2);
        } catch (Exception e) {
            Assert.fail("Exception thrown parsing partitioning key string");
        }
    }
    
    @Test
    public void testInvalidStringForm() {
        try {
            new PartitioningKey("bogus format");
            Assert.fail("String should have triggered exception");
        } catch (Exception e) {
            
        }
        
        try {
            new PartitioningKey("key-xxx");
            Assert.fail("String should have triggered exception");
        } catch (Exception e) {
            
        }
        
        
    }
}
