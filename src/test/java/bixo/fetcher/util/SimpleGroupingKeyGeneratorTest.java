package bixo.fetcher.util;

import junit.framework.Assert;

import org.apache.http.HttpStatus;
import org.junit.Test;
import org.mortbay.http.HttpServer;

import bixo.datum.UrlDatum;
import bixo.fetcher.FixedStatusResponseHandler;
import bixo.fetcher.ResourcesResponseHandler;
import bixo.fetcher.simulation.SimulationWebServer;
import bixo.utils.GroupingKey;

public class SimpleGroupingKeyGeneratorTest extends SimulationWebServer {

    @Test
    public void testNoRobots() throws Exception {
        HttpServer server = startServer(new FixedStatusResponseHandler(HttpStatus.SC_NOT_FOUND), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator("user agent");
        String url = "http://localhost:8089/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        server.stop();

        Assert.assertEquals("127.0.0.1-30000", key);
    }
    
    @Test
    public void testDeferredVisit() throws Exception {
        HttpServer server = startServer(new FixedStatusResponseHandler(HttpStatus.SC_INTERNAL_SERVER_ERROR), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator("user agent");
        String url = "http://localhost:8089/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        server.stop();

        Assert.assertEquals(GroupingKey.DEFERRED_GROUPING_KEY, key);
    }
    
    @Test
    public void testAllowedDisallowedURL() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler("/groupingkeytests"), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator("testAgent");
        
        String url = "http://localhost:8089/allowed/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        Assert.assertEquals("127.0.0.1-10000", key);
        
        url = "http://localhost:8089/disallowed/page.html";
        urlDatum = new UrlDatum(url);
        key = keyGen.getGroupingKey(urlDatum);
        Assert.assertEquals(GroupingKey.BLOCKED_GROUPING_KEY, key);

        server.stop();
    }
    
    @Test
    public void testUsingPLD() throws Exception {
        HttpServer server = startServer(new FixedStatusResponseHandler(HttpStatus.SC_NOT_FOUND), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator("user agent", null, true);
        String url = "http://localhost:8089/page.html";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        server.stop();

        Assert.assertEquals("localhost-30000", key);
    }
    
    @Test
    public void testTwitterWithPLD() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler("/twitter"), 8089);
        SimpleGroupingKeyGenerator keyGen = new SimpleGroupingKeyGenerator("testAgent", null, true);
        
        String url = "http://localhost:8089/";
        UrlDatum urlDatum = new UrlDatum(url);
        String key = keyGen.getGroupingKey(urlDatum);
        server.stop();

        Assert.assertEquals("localhost-30000", key);
    }
}
