package bixo.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import bixo.robots.RobotRules;

public class GroupingKeyTest {

    @Test
    public void testFormattingKey() {
        assertEquals("domain.com-unset", GroupingKey.makeGroupingKey("domain.com", RobotRules.UNSET_CRAWL_DELAY));
        assertEquals("domain.com-30000", GroupingKey.makeGroupingKey("domain.com", 30000));
    }
    
    @Test
    public void testExtractingDomain() {
        assertEquals("domain.com", GroupingKey.getDomainFromKey("domain.com-unset"));
        assertEquals("domain.com", GroupingKey.getDomainFromKey("domain.com-30000"));
    }
    
    @Test
    public void testExtractingCrawlDelay() {
        assertEquals(RobotRules.UNSET_CRAWL_DELAY, GroupingKey.getCrawlDelayFromKey("000001-domain.com-unset"));
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
