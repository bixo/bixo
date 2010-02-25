package bixo.fetcher.http;

import org.junit.Assert;
import org.junit.Test;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.IOFetchException;
import bixo.exceptions.RedirectFetchException;
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
    
    @Test
    public final void testHeadRequest() throws Exception {
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, ConfigUtils.BIXO_IT_AGENT);
        String url = "http://www.facebookxxxxx.com";
        
        try {
            fetcher.head(new ScoredUrlDatum(url));
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOFetchException);
        }

        url = "http://www.google.com";
        FetchedDatum datum = fetcher.head(new ScoredUrlDatum(url));
        Assert.assertEquals(0, datum.getContentLength());
        
        url = "http://bit.ly/cpZsr1";
        datum = fetcher.head(new ScoredUrlDatum(url));
        Assert.assertTrue(datum.getNumRedirects() > 0);
        Assert.assertEquals("http://bixolabs.com", datum.getNewBaseUrl());
    }
    
    @Test
    public void testHeadManyRedirects() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxContentSize(0);
        policy.setMaxRedirects(1);
        policy.setMaxConnectionsPerHost(20);
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_IT_AGENT);
        fetcher.setMaxRetryCount(2);
        fetcher.setConnectionTimeout(10 * 1000);
        fetcher.setSocketTimeout(10 * 1000);

        String url = "http://bit.ly/1547jR";
        try {
            fetcher.head(new ScoredUrlDatum(url));
            Assert.fail("Should have too many redirects exception");
        } catch (RedirectFetchException e) {
             Assert.assertEquals("http://www.biztools.com/Templates/Article.aspx?id=19416", e.getRedirectedUrl());
        }
    }
}
