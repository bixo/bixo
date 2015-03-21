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
package bixo.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import bixo.config.BaseFetchJobPolicy;

public class GroupingKeyTest {

    @Test
    public void testFormattingKey() {
        assertEquals("domain.com-unset", GroupingKey.makeGroupingKey("domain.com", BaseFetchJobPolicy.UNSET_CRAWL_DELAY));
        assertEquals("domain.com-30000", GroupingKey.makeGroupingKey("domain.com", 30000));
    }
    
    @Test
    public void testExtractingDomain() {
        assertEquals("domain.com", GroupingKey.getDomainFromKey("domain.com-unset"));
        assertEquals("domain.com", GroupingKey.getDomainFromKey("domain.com-30000"));
    }
    
    @Test
    public void testExtractingCrawlDelay() {
        assertEquals(BaseFetchJobPolicy.UNSET_CRAWL_DELAY, GroupingKey.getCrawlDelayFromKey("000001-domain.com-unset"));
        assertEquals(30000, GroupingKey.getCrawlDelayFromKey("domain.com-30000"));
    }
    
    @Test
    public void testFunkyDomainNames() {
        assertEquals("domain-name.com", GroupingKey.getDomainFromKey("domain-name.com-unset"));
    }
    
    @Test
    public void testInvalidKey() {
        try {
            GroupingKey.getCrawlDelayFromKey("domain.com-");
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // Valid
        }
        
        try {
            GroupingKey.getDomainFromKey("-30000");
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // Valid
        }
    }
}
