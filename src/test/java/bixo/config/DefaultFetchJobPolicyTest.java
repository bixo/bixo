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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import bixo.config.BaseFetchJobPolicy.FetchSetInfo;
import bixo.datum.ScoredUrlDatum;

public class DefaultFetchJobPolicyTest {

    @Test
    public void testRegularUsage() throws Exception {
        final int maxUrlsPerSet = 2;
        final int maxUrlsPerServer = 3;
        
        BaseFetchJobPolicy policy = new DefaultFetchJobPolicy(maxUrlsPerSet, maxUrlsPerServer, BaseFetchJobPolicy.DEFAULT_CRAWL_DELAY);
        
        final int crawlDelay = 10000;
        policy.startFetchSet("groupingKey", crawlDelay);
        
        // Should be nothing yet.
        assertNull(policy.endFetchSet());
        
        assertNull(policy.nextFetchSet(new ScoredUrlDatum("url1")));
        
        FetchSetInfo setInfo = policy.nextFetchSet(new ScoredUrlDatum("url2"));
        assertNotNull(setInfo);
        assertEquals(2, setInfo.getUrls().size());
        assertEquals("url1", setInfo.getUrls().get(0).getUrl());
        assertEquals("url2", setInfo.getUrls().get(1).getUrl());
        assertEquals(crawlDelay * 2, setInfo.getFetchDelay());
        
        FetchSetInfo setInfo2 = policy.nextFetchSet(new ScoredUrlDatum("url3"));
        assertNotNull(setInfo2);
        assertEquals(1, setInfo.getUrls().size());
        assertEquals("url3", setInfo.getUrls().get(0).getUrl());
        
        assertNull(policy.endFetchSet());
    }
    
    @Test
    public void testSkipping() throws Exception {
        final int maxUrlsPerSet = 1;
        final int maxUrlsPerServer = 1;
        
        BaseFetchJobPolicy policy = new DefaultFetchJobPolicy(maxUrlsPerSet, maxUrlsPerServer, BaseFetchJobPolicy.DEFAULT_CRAWL_DELAY);
        
        final int crawlDelay = 10000;
        policy.startFetchSet("groupingKey", crawlDelay);
        
        FetchSetInfo setInfo = policy.nextFetchSet(new ScoredUrlDatum("url1"));
        assertNotNull(setInfo);
        assertEquals(1, setInfo.getUrls().size());
        assertFalse(setInfo.isSkipping());
        
        assertNull(policy.nextFetchSet(new ScoredUrlDatum("url2")));
        assertNull(policy.nextFetchSet(new ScoredUrlDatum("url3")));
        assertNull(policy.nextFetchSet(new ScoredUrlDatum("url4")));
        assertNull(policy.nextFetchSet(new ScoredUrlDatum("url5")));

        setInfo = policy.endFetchSet();
        assertNotNull(setInfo);
        assertEquals(4, setInfo.getUrls().size());
        assertTrue(setInfo.isSkipping());
    }
    
    @Test
    public void testSortKey() throws Exception {
        final int maxUrlsPerSet = 1;
        final int maxUrlsPerServer = 100;
        
        BaseFetchJobPolicy policy = new DefaultFetchJobPolicy(maxUrlsPerSet, maxUrlsPerServer, BaseFetchJobPolicy.DEFAULT_CRAWL_DELAY);
        
        final int crawlDelay = 10000;
        policy.startFetchSet("groupingKey", crawlDelay);
        
        long curSortKey = 0;
        for (int i = 0; i < maxUrlsPerServer; i++) {
            FetchSetInfo setInfo = policy.nextFetchSet(new ScoredUrlDatum("url" + i));
            assertNotNull(setInfo);
            assertTrue(setInfo.getSortKey() > curSortKey);
            curSortKey = setInfo.getSortKey();
        }
    }
    
}
