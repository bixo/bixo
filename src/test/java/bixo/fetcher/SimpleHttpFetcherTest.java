package bixo.fetcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.http.HttpStatus;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.http.handler.AbstractHttpHandler;

import bixo.config.DefaultFetchJobPolicy;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.RedirectMode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.AbortedFetchException;
import bixo.exceptions.AbortedFetchReason;
import bixo.exceptions.IOFetchException;
import bixo.exceptions.RedirectFetchException;
import bixo.exceptions.RedirectFetchException.RedirectExceptionReason;
import bixo.fetcher.simulation.SimulationWebServer;
import bixo.utils.ConfigUtils;

public class SimpleHttpFetcherTest extends SimulationWebServer {
    
    @SuppressWarnings("serial")
    private class RedirectResponseHandler extends AbstractHttpHandler {
        
        private boolean _permanent;
        
        public RedirectResponseHandler() {
            this(false);
        }
        
        public RedirectResponseHandler(boolean permanent) {
            super();
            _permanent = permanent;
        }
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            if (pathInContext.endsWith("base")) {
                if (_permanent) {
                    // Can't use sendRedirect, as that forces it to be a temp redirect.
                    response.setStatus(HttpStatus.SC_MOVED_PERMANENTLY);
                    response.addField("Location", "http://localhost:8089/redirect");
                    request.setHandled(true);
                } else {
                    response.sendRedirect("http://localhost:8089/redirect");
                }
            } else {
                response.setStatus(HttpStatus.SC_OK);
                response.setContentType("text/plain");

                String content = "redirected";
                response.setContentLength(content.length());
                response.getOutputStream().write(content.getBytes());
            }
        }
    }

    @SuppressWarnings("serial")
    private class LanguageResponseHandler extends AbstractHttpHandler {
        
        private String _englishContent;
        private String _foreignContent;
        
        public LanguageResponseHandler(String englishContent, String foreignContent) {
            _englishContent = englishContent;
            _foreignContent = foreignContent;
        }

        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            String language = request.getField(HttpHeaderNames.ACCEPT_LANGUAGE);
            String content;
            if ((language != null) && (language.contains("en"))) {
                content = _englishContent;
            } else {
                content = _foreignContent;
            }

            response.setStatus(HttpStatus.SC_OK);
            response.setContentType("text/plain");

            response.setContentLength(content.length());
            response.getOutputStream().write(content.getBytes());
        }
    }

    @SuppressWarnings("serial")
    private class MimeTypeResponseHandler extends AbstractHttpHandler {
        
        private String _mimeType;
        
        public MimeTypeResponseHandler(String mimeType) {
            _mimeType = mimeType;
        }

        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            String content = "test";
            response.setStatus(HttpStatus.SC_OK);
            if (_mimeType != null) {
                response.setContentType(_mimeType);
            }
            
            response.setContentLength(content.length());
            response.getOutputStream().write(content.getBytes());
        }
    }

    @Test
    public final void testConnectionTimeout() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8088/simple-page.html";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            fail("Exception not thrown");
        } catch (IOFetchException e) {
            assertTrue(e.getCause() instanceof HttpHostConnectException);
        } finally {
            server.stop();
        }
    }
    
    @Test
    public final void testStaleConnection() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        SocketListener sl = (SocketListener)server.getListeners()[0];
        sl.setLingerTimeSecs(-1);

        BaseFetcher fetcher = new SimpleHttpFetcher(1, ConfigUtils.BIXO_TEST_AGENT);
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

        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);

        String url = "http://localhost:8089/test.html";
        try {
            fetcher.get(new ScoredUrlDatum(url));
            fail("Aborted fetch exception not thrown");
        } catch (AbortedFetchException e) {
            assertEquals(AbortedFetchReason.SLOW_RESPONSE_RATE, e.getAbortReason());
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

        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);

        String url = "http://localhost:8089/test.html";
        fetcher.get(new ScoredUrlDatum(url));
        server.stop();
    }
    
    @Test
    public final void testLargeContent() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new RandomResponseHandler(policy.getMaxContentSize() * 2), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/test.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();

        assertTrue("Content size should be truncated", result.getContentLength() <= policy.getMaxContentSize());
    }
    
    @Test
    public final void testTruncationWithKeepAlive() throws Exception {
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);

        FetcherPolicy policy = new FetcherPolicy();
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        fetcher.setDefaultMaxContentSize(1000);
        fetcher.setMaxContentSize("image/png", 5000);
        ScoredUrlDatum datumToFetch = new ScoredUrlDatum("http://localhost:8089/karlie.html");
        
        FetchedDatum result1 = fetcher.get(datumToFetch);
        FetchedDatum result2 = fetcher.get(datumToFetch);
        
        // Verify that we got the same data from each fetch request.
        assertEquals(1000, result1.getContentLength());
        assertEquals(1000, result2.getContentLength());
        byte[] bytes1 = result1.getContentBytes();
        byte[] bytes2 = result2.getContentBytes();
        for (int i = 0; i < bytes1.length; i++) {
            assertEquals(bytes1[i], bytes2[i]);
        }

        datumToFetch = new ScoredUrlDatum("http://localhost:8089/bixolabs_mining.png");
        FetchedDatum result3 = fetcher.get(datumToFetch);
        assertTrue(result3.getContentLength() > 1000);
        
        fetcher.setMaxContentSize("image/png", 1500);
        try {
            fetcher.get(datumToFetch);
            fail("Aborted fetch exception not thrown");
        } catch (AbortedFetchException e) {
            Assert.assertEquals(AbortedFetchReason.CONTENT_SIZE, e.getAbortReason());
        }

        server.stop();
    }
    
    @Test
    public final void testLargeHtml() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/karlie.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();

        assertTrue("Content size should be truncated", result.getContentLength() <= policy.getMaxContentSize());

    }
    
    @Test
    public final void testContentTypeHeader() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/simple-page.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();
        
        String contentType = result.getHeaders().getFirst(HttpHeaderNames.CONTENT_TYPE);
        assertNotNull(contentType);
        assertEquals("text/html", contentType);
    }
    
    @Test
    public final void testTempRedirectHandling() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new RedirectResponseHandler(), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/base";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();

        assertEquals("Redirected URL", "http://localhost:8089/redirect", result.getFetchedUrl());
        assertNull(result.getNewBaseUrl());
        assertEquals(1, result.getNumRedirects());
    }
    
    @Test
    public final void testPermRedirectHandling() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new RedirectResponseHandler(true), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/base";
        ScoredUrlDatum scoredUrl = new ScoredUrlDatum(url);
        scoredUrl.setPayloadValue("payload-field-1", 1);
        FetchedDatum result = fetcher.get(scoredUrl);
        server.stop();

        assertEquals("Redirected URL", "http://localhost:8089/redirect", result.getFetchedUrl());
        assertEquals("New base URL", "http://localhost:8089/redirect", result.getNewBaseUrl());
        assertEquals(1, result.getNumRedirects());
        assertEquals(1, result.getPayloadValue("payload-field-1"));
    }
    
    @Test
    public final void testRedirectPolicy() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setRedirectMode(RedirectMode.FOLLOW_TEMP);
        HttpServer server = startServer(new RedirectResponseHandler(true), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/base";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            fail("Exception should have been thrown");
        } catch (RedirectFetchException e) {
            assertEquals("Redirected URL", "http://localhost:8089/redirect", e.getRedirectedUrl());
            assertEquals(RedirectExceptionReason.PERM_REDIRECT_DISALLOWED, e.getReason());
        } finally {
            server.stop();
        }
        
        // Now try setting the mode to follow none
        policy.setRedirectMode(RedirectMode.FOLLOW_NONE);
        server = startServer(new RedirectResponseHandler(false), 8089);
        fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            fail("Exception should have been thrown");
        } catch (RedirectFetchException e) {
            assertEquals("Redirected URL", "http://localhost:8089/redirect", e.getRedirectedUrl());
            assertEquals(RedirectExceptionReason.TEMP_REDIRECT_DISALLOWED, e.getReason());
        } finally {
            server.stop();
        }

    }
    
    @Test
    public final void testAcceptLanguage() throws Exception {
        final String englishContent = "English";
        final String foreignContent = "Foreign";
        
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new LanguageResponseHandler(englishContent, foreignContent), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();
        String contentStr = new String(result.getContentBytes(), 0, result.getContentLength());
        assertTrue( englishContent.equals(contentStr));
    }

    @Test
    public final void testMimeTypeFiltering() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        Set<String> validMimeTypes = new HashSet<String>();
        validMimeTypes.add("text/html");
        policy.setValidMimeTypes(validMimeTypes);

        HttpServer server = startServer(new MimeTypeResponseHandler("text/xml"), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            fail("Fetch should have failed");
        } catch (AbortedFetchException e) {
            assertEquals(AbortedFetchReason.INVALID_MIMETYPE, e.getAbortReason());
        } finally {
            server.stop();
        }
    }

    @Test
    public final void testMimeTypeFilteringNoContentType() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        Set<String> validMimeTypes = new HashSet<String>();
        validMimeTypes.add("text/html");
        validMimeTypes.add(""); // We want unknown (not reported) mime-types too.
        policy.setValidMimeTypes(validMimeTypes);

        HttpServer server = startServer(new MimeTypeResponseHandler(null), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
        } catch (AbortedFetchException e) {
            fail("Fetch should not have failed if no mime-type is specified");
        } finally {
            server.stop();
        }
    }

    @Test
    public final void testMimeTypeFilteringWithCharset() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        Set<String> validMimeTypes = new HashSet<String>();
        validMimeTypes.add("text/html");
        policy.setValidMimeTypes(validMimeTypes);

        HttpServer server = startServer(new MimeTypeResponseHandler("text/html; charset=UTF-8"), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
        } catch (AbortedFetchException e) {
            fail("Fetch should have worked");
        } finally {
            server.stop();
        }
    }

    @Test
    public final void testHostAddress() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        HttpServer server = startServer(new ResourcesResponseHandler(), 8089);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        String url = "http://localhost:8089/simple-page.html";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        server.stop();
        
        String hostAddress = result.getHostAddress();
        assertNotNull(hostAddress);
        assertEquals("127.0.0.1", hostAddress);
    }
    
    @Test
    public final void testAcceptEncoding() throws Exception {
        FetcherPolicy policy = new FetcherPolicy();
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        
        System.out.println(fetcher.getAcceptEncoding());
        
        final String acceptEncoding = "bogus";
        fetcher.setAcceptEncoding(acceptEncoding);
        assertEquals(acceptEncoding, fetcher.getAcceptEncoding());
    }
    
}
