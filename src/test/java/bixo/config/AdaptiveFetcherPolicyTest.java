/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package bixo.config;

import org.junit.Assert;
import org.junit.Test;

import bixo.fetcher.FetchRequest;


public class AdaptiveFetcherPolicyTest {
    
    @Test
    public void testMinDelayLimit() {
        // Target end is now + 10 seconds, and our min delay is 1 second.
        long now = System.currentTimeMillis();
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(now + (10 * 1000), 1000L);
        
        // Ask for more than we can get in the remaining time, so it has to use a 5 minutes
        // window.
        final long crawlDelay = 30 * 1000L;
        FetchRequest request = policy.getFetchRequest(now, crawlDelay, 1000);
        
        // No way we can get more than 300.
        Assert.assertEquals(300, request.getNumUrls());
    }
    
    @Test
    public void testFiveMinuteWindow() {
        // Target end is now + 600 seconds (10 minutes), and our min delay is 1 second.
        long now = System.currentTimeMillis();
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(now + (600 * 1000), 1000L);

        // Ask for more than will fit in the duration assuming a 30 second delay, but less than what
        // would be constrained by the 1 second min delay.
        final long crawlDelay = 30 * 1000L;
        FetchRequest request = policy.getFetchRequest(now, crawlDelay, 100);
        
        // Our per-request rate is about 600/100 = 6 seconds. So in 5 minutes (the default
        // window) we should get back 50
        Assert.assertEquals(50, request.getNumUrls());
        
        // Our next fetch time should be around 5 minutes.
        long deltaFromTarget = Math.abs(request.getNextRequestTime() - (now + (5 * 60 * 1000L)));
        Assert.assertTrue(deltaFromTarget < 100);
    }
    
    @Test
    public void testPastEndOfTargetDuration() {
        // If we're at the end of the crawl, we still want to be fetching as otherwise we'll
        // never end.
        long now = System.currentTimeMillis();
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(now - 1000, 10 * 1000L);
        
        // Ask for more than we can get in the remaining time, so it has to use a 5 minutes
        // window.
        final long crawlDelay = 30 * 1000L;
        FetchRequest request = policy.getFetchRequest(now, crawlDelay, 1000);
        
        Assert.assertEquals(30, request.getNumUrls());

        // Our next fetch time should be around 5 minutes.
        long deltaFromTarget = Math.abs(request.getNextRequestTime() - (now + (5 * 60 * 1000L)));
        Assert.assertTrue(deltaFromTarget < 100);
    }
}
