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
        
        for (int i = 0; i < 100; i++) {
            long delay = policy.getCrawlDelay();
            Assert.assertTrue(delay >= 5);
            Assert.assertTrue(delay <= 15);
        }
    }
    
    @Test
    public void testEmptyFetchList() {
        FakeUserFetcherPolicy policy = new FakeUserFetcherPolicy();
        FetchRequest request = policy.getFetchRequest(System.currentTimeMillis(), 30 * 1000L, 0);
        Assert.assertEquals(0, request.getNumUrls());
    }
    
    @Test
    public void testRandomDelay() {
        FakeUserFetcherPolicy policy = new FakeUserFetcherPolicy(60 * 1000L);
        
        int delayCounts[] = new int[70];
        for (int i = 0; i < 70; i++) {
            delayCounts[i] = 0;
        }
        
        for (int i = 0; i < 100; i++) {
            long curTime = System.currentTimeMillis();
            
            // Pass in longer crawl delay, that we'll ignore
            FetchRequest request = policy.getFetchRequest(curTime, 100 * 1000L, 100);
            Assert.assertEquals(1, request.getNumUrls());
            int delayInSeconds = (int)(request.getNextRequestTime() - curTime)/1000;
            
            Assert.assertTrue("Delay #" + i + " must be at least 5 seconds: " + delayInSeconds, delayInSeconds >= 30);
            Assert.assertTrue(delayInSeconds <= 91);
            delayCounts[delayInSeconds - 30] += 1;
        }
        
        for (int i = 0; i < 70; i++) {
            Assert.assertTrue(delayCounts[i] < 10);
        }
    }
}
