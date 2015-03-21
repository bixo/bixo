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

// TODO Move these tests into DefaultFetchJobPolicyTest, once it supports settings for
// during the fetch portion of the job (versus just the set generation phase).

public class FetcherPolicyTest {

    @Test
    public void testZeroCrawlDelay() {
        FetcherPolicy policy = new FetcherPolicy(FetcherPolicy.NO_MIN_RESPONSE_RATE,
                        FetcherPolicy.DEFAULT_MAX_CONTENT_SIZE, FetcherPolicy.NO_CRAWL_END_TIME, 0,
                        FetcherPolicy.DEFAULT_MAX_REDIRECTS);
        policy.setMaxRequestsPerConnection(100);
        
        try {
//            FetchRequest request = policy.getFetchRequest(System.currentTimeMillis(), 0, 100);
//            Assert.assertEquals(100, request.getNumUrls());
//            Assert.assertTrue(request.getNextRequestTime() <= System.currentTimeMillis());
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
    
}
