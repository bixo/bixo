package bixo.fetcher.http;

import org.junit.Assert;
import org.junit.Test;

import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.IOFetchException;

public class SimpleHttpFetcherIntegrationTest {
    private static final String USER_AGENT = "Bixo test agent";
    
    @Test
    public final void testNoDomain() throws Exception {
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, USER_AGENT);
        String url = "http://www.facebookxxxxx.com";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOFetchException);
        }
    }
    
    
}
