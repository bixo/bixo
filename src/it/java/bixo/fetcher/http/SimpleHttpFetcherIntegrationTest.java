package bixo.fetcher.http;

import org.junit.Assert;
import org.junit.Test;

import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.IOFetchException;
import bixo.utils.ConfigUtils;

public class SimpleHttpFetcherIntegrationTest {
    
    @Test
    public final void testNoDomain() {
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, ConfigUtils.BIXO_IT_AGENT);
        String url = "http://www.facebookxxxxx.com";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOFetchException);
        }
    }
    
}
