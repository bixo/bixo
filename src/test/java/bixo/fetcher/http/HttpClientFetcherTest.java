package bixo.fetcher.http;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpListener;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.RandomResponseHandler;
import bixo.fetcher.ResourcesResponseHandler;
import bixo.fetcher.http.HttpClientFetcher;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.simulation.SimulationWebServer;

public class HttpClientFetcherTest extends SimulationWebServer {
    private static final String USER_AGENT = "Bixo test agent";
    
    @SuppressWarnings("serial")
    private class TerminatingHandler extends ResourcesResponseHandler {
        
        private boolean _firstRequest = true;
        private HttpServer _server;
        
        public void setServer(HttpServer server) {
            _server = server;
        }
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            if (_firstRequest) {
                _firstRequest = false;
                super.handle(pathInContext, pathParams, request, response);
            } else {
                // Need to terminate the connection.
                response.getOutputStream().close();
                
                try {
                    _server.stop(false);
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    @Test
    public final void testNoDomain() throws Exception {
        IHttpFetcher fetcher = new HttpClientFetcher(1, USER_AGENT);
        String url = "http://www.facebookxxxxx.com";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));

        Assert.assertEquals(FetchedDatum.SC_UNKNOWN, result.getHttpStatus());
        Assert.assertTrue(result.getHttpMsg().length() > 0);
    }
    
    @Test
    public final void testStaleConnection() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        SocketListener sl = (SocketListener)server.getListeners()[0];
        sl.setLingerTimeSecs(-1);

        IHttpFetcher fetcher = new HttpClientFetcher(1, USER_AGENT);
        String url = "http://localhost:8089/simple-page.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        Assert.assertEquals(FetchStatusCode.FETCHED, result.getStatusCode());
        
        // TODO KKr - control keep-alive (linger?) value for Jetty, so we can set it
        // to something short and thus make this sleep delay much shorter.
        Thread.sleep(2000);
        
        result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();
        
        Assert.assertEquals(FetchStatusCode.FETCHED, result.getStatusCode());
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

        IHttpFetcher fetcher = new HttpClientFetcher(1, policy, USER_AGENT);

        String url = "http://localhost:8089/test.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, null, 1d, null));
        server.stop();

        // Since our SlowResponseHandler is returning 10000 bytes in 1 second, we should
        // get a aborted result.
        FetchStatusCode statusCode = result.getStatusCode();
        Assert.assertEquals(FetchStatusCode.ABORTED, statusCode);
    }

    @Test
    public final void testNotTerminatingSlowServers() throws Exception {
        // Return 1K bytes at 2K bytes/second - would normally trigger an
        // error.
        HttpServer server = startServer(new RandomResponseHandler(1000, 500), 8089);

        // Set up for no minimum response rate.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(FetcherPolicy.NO_MIN_RESPONSE_RATE);

        IHttpFetcher fetcher = new HttpClientFetcher(1, policy, USER_AGENT);

        String url = "http://localhost:8089/test.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, null, 1d, null));
        server.stop();

        FetchStatusCode statusCode = result.getStatusCode();
        Assert.assertEquals(FetchStatusCode.FETCHED, statusCode);
    }
    
    @Test
    public final void testLargeContent() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new RandomResponseHandler(policy.getMaxContentSize() * 2), 8089);
        IHttpFetcher fetcher = new HttpClientFetcher(1, policy, USER_AGENT);
        String url = "http://localhost:8089/test.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, null, 1d, null));
        server.stop();

        Assert.assertTrue("Content size should be truncated", result.getContent().getLength() <= policy.getMaxContentSize());
    }
    
    @Test
    public final void testLargeHtml() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        IHttpFetcher fetcher = new HttpClientFetcher(1, policy, USER_AGENT);
        String url = "http://localhost:8089/karlie.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, null, 1d, null));
        server.stop();

        Assert.assertTrue("Content size should be truncated", result.getContent().getLength() <= policy.getMaxContentSize());

    }
    
    @Test
    public final void testContentTypeHeader() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        IHttpFetcher fetcher = new HttpClientFetcher(1, policy, USER_AGENT);
        String url = "http://localhost:8089/simple-page.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, null, 1d, null));
        server.stop();
        
        String contentType = result.getHeaders().getFirst(IHttpHeaders.CONTENT_TYPE);
        Assert.assertNotNull(contentType);
        Assert.assertEquals("text/html", contentType);
    }
    
}
