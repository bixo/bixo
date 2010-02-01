package bixo.utils;

import org.junit.Test;

import bixo.fetcher.http.IRobotRules;
import static org.junit.Assert.*;

public class GroupingKeyTest {

    @Test
    public void testFormattingKey() {
        assertEquals("000001-domain.com-unset", GroupingKey.makeGroupingKey(1, "domain.com", IRobotRules.UNSET_CRAWL_DELAY));
        assertEquals("000010-domain.com-30000", GroupingKey.makeGroupingKey(10, "domain.com", 30000));
    }
    
    @Test
    public void testExtractingDomain() {
        assertEquals("domain.com", GroupingKey.getDomainFromKey("000001-domain.com-unset"));
        assertEquals("domain.com", GroupingKey.getDomainFromKey("000001-domain.com-30000"));
    }
    
    @Test
    public void testExtractingCrawlDelay() {
        assertEquals(IRobotRules.UNSET_CRAWL_DELAY, GroupingKey.getCrawlDelayFromKey("000001-domain.com-unset"));
        assertEquals(30000, GroupingKey.getCrawlDelayFromKey("000001-domain.com-30000"));
    }
    
    @Test
    public void testExtractingCount() {
        assertEquals(1, GroupingKey.getCountFromKey("000001-domain.com-30000"));
        assertEquals(999, GroupingKey.getCountFromKey("000999-domain.com-unset"));
    }
    
    @Test
    public void testFunkyDomainNames() {
        assertEquals("domain-name.com", GroupingKey.getDomainFromKey("000001-domain-name.com-unset"));
    }
    
    @Test
    public void testInvalidKey() {
        try {
            GroupingKey.getCrawlDelayFromKey("000001-domain.com-");
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // Valid
        }
        
        try {
            GroupingKey.getCrawlDelayFromKey("domain.com-30000");
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // Valid
        }
        
        try {
            GroupingKey.getDomainFromKey("domain.com-30000");
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // Valid
        }
        
        try {
            GroupingKey.getDomainFromKey("000001-domain.com-");
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // Valid
        }
    }
}
