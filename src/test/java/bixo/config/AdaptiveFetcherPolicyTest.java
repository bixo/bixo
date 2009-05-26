package bixo.config;

import org.junit.Assert;
import org.junit.Test;

import bixo.fetcher.FetchRequest;


public class AdaptiveFetcherPolicyTest {
    
    @Test
    public void testMinDelayLimit() {
        // Target end is now + 10 seconds, and our min delay is 1 second.
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + (10 * 1000), 1);
        
        // Ask for more than we can get in the remaining time.
        FetchRequest request = policy.getFetchRequest(100);
        
        // No way we can get more than 11. Might wind up being 10, depending on how long
        // it takes this code to execute.
        Assert.assertTrue(request.getNumUrls() <= 11);
        Assert.assertTrue(request.getNumUrls() >= 10);
    }
    
    @Test
    public void testFiveMinuteWindow() {
        // Target end is now + 600 seconds (10 minutes), and our min delay is 1 second.
        long now = System.currentTimeMillis();
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(now + (600 * 1000), 1);

        // Ask for more than will fit in the duration assuming a 30 second delay, but less than what
        // would be constrained by the 1 second min delay.
        FetchRequest request = policy.getFetchRequest(100);
        
        // Our per-request rate is about 600/100 = 6 seconds. So in 5 minutes (the default
        // window) we should get back 50 + 1 = 51 (fencepost), though potentially one less
        // due to elapsed time for the above code.
        Assert.assertTrue(request.getNumUrls() <= 51);
        Assert.assertTrue(request.getNumUrls() >= 50);
        
        // Our next fetch time should be around 5 minutes.
        long deltaFromTarget = Math.abs(request.getNextRequestTime() - (now + (5 * 60 * 1000L)));
        Assert.assertTrue(deltaFromTarget < 100);
    }
}
