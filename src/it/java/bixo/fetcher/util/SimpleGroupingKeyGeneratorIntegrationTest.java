package bixo.fetcher.util;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import bixo.datum.UrlDatum;
import bixo.utils.ConfigUtils;
import bixo.utils.GroupingKey;

public class SimpleGroupingKeyGeneratorIntegrationTest {
    
    @Test
    public void testBogusHostname() throws IOException {
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(ConfigUtils.BIXO_TEST_AGENT);
        
        String url = "http://totalbogusdomainxxx.com";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        Assert.assertEquals(GroupingKey.UNKNOWN_HOST_GROUPING_KEY, key);
    }
    
}
