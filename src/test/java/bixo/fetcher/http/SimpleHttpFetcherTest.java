package bixo.fetcher.http;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.AbortedFetchException;
import bixo.exceptions.AbortedFetchReason;
import bixo.exceptions.IOFetchException;
import bixo.fetcher.RandomResponseHandler;
import bixo.fetcher.ResourcesResponseHandler;
import bixo.fetcher.simulation.SimulationWebServer;

public class SimpleHttpFetcherTest extends SimulationWebServer {
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
    
    @Test
    public final void testStaleConnection() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        SocketListener sl = (SocketListener)server.getListeners()[0];
        sl.setLingerTimeSecs(-1);

        IHttpFetcher fetcher = new SimpleHttpFetcher(1, USER_AGENT);
        String url = "http://localhost:8089/simple-page.html";
        fetcher.get(new ScoredUrlDatum(url));
        
        // TODO KKr - control keep-alive (linger?) value for Jetty, so we can set it
        // to something short and thus make this sleep delay much shorter.
        Thread.sleep(2000);
        
        fetcher.get(new ScoredUrlDatum(url));
        server.stop();
    }
    
    @Test
    public final void testSlowServerTermination() throws Exception {
        // Need to read in more than 2 8K blocks currently, due to how
        // HttpClientFetcher
        // is designed...so use 20K bytes. And the duration is 2 seconds, so 10K
        // bytes/sec.
        HttpServer server = startServer(new RandomResponseHandler(20000, 2 * 1000L), 8089);

        // Set up for a minimum response rate of 20000 bytes/second.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(20000);

        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, USER_AGENT);

        String url = "http://localhost:8089/test.html";
        try {
            fetcher.get(new ScoredUrlDatum(url));
            Assert.fail("Aborted fetch exception not thrown");
        } catch (AbortedFetchException e) {
            Assert.assertEquals(AbortedFetchReason.SLOW_RESPONSE_RATE, e.getAbortReason());
        }
        server.stop();
    }

    @Test
    public final void testNotTerminatingSlowServers() throws Exception {
        // Return 1K bytes at 2K bytes/second - would normally trigger an
        // error.
        HttpServer server = startServer(new RandomResponseHandler(1000, 500), 8089);

        // Set up for no minimum response rate.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(FetcherPolicy.NO_MIN_RESPONSE_RATE);

        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, USER_AGENT);

        String url = "http://localhost:8089/test.html";
        fetcher.get(new ScoredUrlDatum(url));
        server.stop();
    }
    
    @Test
    public final void testLargeContent() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new RandomResponseHandler(policy.getMaxContentSize() * 2), 8089);
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, USER_AGENT);
        String url = "http://localhost:8089/test.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();

        Assert.assertTrue("Content size should be truncated", result.getContent().getLength() <= policy.getMaxContentSize());
    }
    
    @Test
    public final void testTruncationWithKeepAlive() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);

        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxContentSize(1000);
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, USER_AGENT);
        
        ScoredUrlDatum datumToFetch = new ScoredUrlDatum("http://localhost:8089/karlie.html");
        
        FetchedDatum result1 = fetcher.get(datumToFetch);
        FetchedDatum result2 = fetcher.get(datumToFetch);

        // Verify that we got the same data from each fetch request.
        Assert.assertEquals(result1.getContent(), result2.getContent());

        server.stop();
    }
    
    @Test
    public final void testLargeHtml() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, USER_AGENT);
        String url = "http://localhost:8089/karlie.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();

        Assert.assertTrue("Content size should be truncated", result.getContent().getLength() <= policy.getMaxContentSize());

    }
    
    @Test
    public final void testContentTypeHeader() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, USER_AGENT);
        String url = "http://localhost:8089/simple-page.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();
        
        String contentType = result.getHeaders().getFirst(IHttpHeaders.CONTENT_TYPE);
        Assert.assertNotNull(contentType);
        Assert.assertEquals("text/html", contentType);
    }
    
}
