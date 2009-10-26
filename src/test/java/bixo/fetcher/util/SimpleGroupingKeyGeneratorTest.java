package bixo.fetcher.util;

import junit.framework.Assert;

import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Test;
import org.mortbay.http.HttpServer;

import bixo.config.UserAgent;
import bixo.datum.UrlDatum;
import bixo.fetcher.FixedStatusResponseHandler;
import bixo.fetcher.RedirectResponseHandler;
import bixo.fetcher.ResourcesResponseHandler;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.simulation.SimulationWebServer;
import bixo.utils.ConfigUtils;
import bixo.utils.GroupingKey;

public class SimpleGroupingKeyGeneratorTest extends SimulationWebServer {
    
    private HttpServer _server;
    
    @After
    public void tearDown() throws InterruptedException {
        if ((_server != null) && _server.isStarted()) {
            _server.stop();
        }
    }
    
    @Test
    public void testNoRobots() throws Exception {
        _server = startServer(new FixedStatusResponseHandler(HttpStatus.SC_NOT_FOUND), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);

        Assert.assertEquals("127.0.0.1-unset", key);
    }
    
    @Test
    public void testDeferredVisit() throws Exception {
        _server = startServer(new FixedStatusResponseHandler(HttpStatus.SC_INTERNAL_SERVER_ERROR), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);

        Assert.assertEquals(GroupingKey.DEFERRED_GROUPING_KEY, key);
    }
    
    @Test
    public void testRobotsHttpsFetchError() throws Exception {
        _server = startServer(new RedirectResponseHandler("/robots.txt", "https://localhost:8089/robots.txt"), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/page.txt";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);

        Assert.assertEquals(GroupingKey.DEFERRED_GROUPING_KEY, key);
    }
    
    @Test
    public void testAllowedDisallowedURL() throws Exception {
        _server = startServer(new ResourcesResponseHandler("/groupingkeytests"), 8089);
        UserAgent userAgent = new UserAgent("testAgent", "testAgent@domain.com", "http://testAgent.domain.com");
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(userAgent);
        
        String url = "http://localhost:8089/allowed/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        Assert.assertEquals("127.0.0.1-10000", key);
        
        url = "http://localhost:8089/disallowed/page.html";
        urlDatum = new UrlDatum(url);
        key = keyGen.getGroupingKey(urlDatum);
        Assert.assertEquals(GroupingKey.BLOCKED_GROUPING_KEY, key);
    }
    
    @Test
    public void testUsingPLD() throws Exception {
        _server = startServer(new FixedStatusResponseHandler(HttpStatus.SC_NOT_FOUND), 8089);
        UserAgent userAgent = new UserAgent("testAgent", "testAgent@domain.com", "http://testAgent.domain.com");
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, userAgent);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(fetcher, true);
        String url = "http://localhost:8089/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);

        Assert.assertEquals("localhost-unset", key);
    }
    
    @Test
    public void testTwitterWithPLD() throws Exception {
        _server = startServer(new ResourcesResponseHandler("/twitter"), 8089);
        UserAgent userAgent = new UserAgent("testAgent", "testAgent@domain.com", "http://testAgent.domain.com");
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, userAgent);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator(fetcher, true);
        
        String url = "http://localhost:8089/";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);

        Assert.assertEquals("localhost-unset", key);
    }
}
