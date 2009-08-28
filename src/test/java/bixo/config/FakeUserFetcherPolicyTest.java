package bixo.config;

import junit.framework.Assert;

import org.junit.Test;

import bixo.fetcher.FetchRequest;


public class FakeUserFetcherPolicyTest {

    @Test
    public void testDefaultDelay() {
        FakeUserFetcherPolicy policy = new FakeUserFetcherPolicy();
        Assert.assertEquals(60 * 1000L, policy.getDefaultCrawlDelay());
    }
    
    @Test
    public void testExplicitDelay() {
        FakeUserFetcherPolicy policy = new FakeUserFetcherPolicy(10);
        Assert.assertEquals(10, policy.getDefaultCrawlDelay());
    }
    
    @Test
    public void testEmptyFetchList() {
        FakeUserFetcherPolicy policy = new FakeUserFetcherPolicy();
        FetchRequest request = policy.getFetchRequest(0);
        Assert.assertEquals(0, request.getNumUrls());
    }
    
    @Test
    public void testRandomDelay() {
        FakeUserFetcherPolicy policy = new FakeUserFetcherPolicy();
        
        int delayCounts[] = new int[70];
        for (int i = 0; i < 70; i++) {
            delayCounts[i] = 0;
        }
        
        for (int i = 0; i < 100; i++) {
            long curTime = System.currentTimeMillis();
            FetchRequest request = policy.getFetchRequest(100);
            Assert.assertEquals(1, request.getNumUrls());
            int delayInSeconds = (int)(request.getNextRequestTime() - curTime)/1000;
            
            Assert.assertTrue("Delay #" + i + " must be at least 30 seconds: " + delayInSeconds, delayInSeconds >= 30);
            Assert.assertTrue(delayInSeconds <= 91);
            delayCounts[delayInSeconds - 30] += 1;
        }
        
        for (int i = 0; i < 70; i++) {
            Assert.assertTrue(delayCounts[i] < 10);
        }
    }
}
