package bixo.config;

import junit.framework.Assert;

import org.junit.Test;

import bixo.fetcher.FetchRequest;

public class FetcherPolicyTest {

    @SuppressWarnings("serial")
    private static class MyFetcherPolicy extends FetcherPolicy {
        public MyFetcherPolicy() {
            super();
        }
    }
    
    @Test
    public void testZeroCrawlDelay() {
        FetcherPolicy policy = new FetcherPolicy(FetcherPolicy.NO_MIN_RESPONSE_RATE,
                        FetcherPolicy.DEFAULT_MAX_CONTENT_SIZE, FetcherPolicy.NO_CRAWL_END_TIME, 0,
                        FetcherPolicy.DEFAULT_MAX_REDIRECTS);
        policy.setMaxRequestsPerConnection(100);
        
        try {
            FetchRequest request = policy.getFetchRequest(System.currentTimeMillis(), 0, 100);
            Assert.assertEquals(100, request.getNumUrls());
            Assert.assertTrue(request.getNextRequestTime() <= System.currentTimeMillis());
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }
    
    @Test
    public void testErrorSettingCrawlDelayInSeconds() {
        try {
            new FetcherPolicy(FetcherPolicy.NO_MIN_RESPONSE_RATE,
                            FetcherPolicy.DEFAULT_MAX_CONTENT_SIZE, FetcherPolicy.NO_CRAWL_END_TIME,
                            30, // Use 30 seconds vs. 30000ms
                            FetcherPolicy.DEFAULT_MAX_REDIRECTS);
            Assert.fail("Should have thrown error with crawl delay of 30");
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testMakingNewPolicy() {
        FetcherPolicy policy = new FetcherPolicy();
        
        FetcherPolicy newPolicy = policy.makeNewPolicy(policy.getCrawlDelay());
        Assert.assertTrue(newPolicy.equals(policy));
    }
    
    @Test
    public void testNotOverridingMakeNewPolicy() {
        FetcherPolicy policy = new MyFetcherPolicy();
        FetcherPolicy newPolicy = policy.makeNewPolicy(policy.getCrawlDelay());
        Assert.assertNotSame(newPolicy.getClass(), policy.getClass());
    }
}
