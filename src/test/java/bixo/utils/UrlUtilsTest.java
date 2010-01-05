package bixo.utils;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.*;
import org.junit.Test;

public class UrlUtilsTest {

    @Test
    public void testQueryParamReplacingQueryParam() throws MalformedURLException {
        URL baseUrl = new URL("http://domain.com?p=1");
        String derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=2");
        assertEquals("http://domain.com/?pid=2", derivedUrl);
    }

    @Test
    public void testQueryParamCase() throws MalformedURLException {
        URL baseUrl = new URL("http://domain.com");
        String derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
        assertEquals("http://domain.com/?pid=1", derivedUrl);
    }

    @Test
    public void testLeadingSlash() throws MalformedURLException {
        URL baseUrl;

        baseUrl = new URL("http://domain.com/path/to/file");
        String derivedUrl = UrlUtils.makeUrl(baseUrl, "/file.html");
        assertEquals("http://domain.com/file.html", derivedUrl);
    }

    @Test
    public void testJavascript() throws MalformedURLException {
        URL baseUrl;

        baseUrl = new URL("http://domain.com/");
        String relativeUrl = "JavaScript:GoURL(dowURL, '')";
        String derivedUrl = UrlUtils.makeUrl(baseUrl, relativeUrl);
        assertEquals(relativeUrl, derivedUrl);
    }
    
    @Test
    public void testLeadingDotSlash() throws MalformedURLException {
        URL baseUrl;

        baseUrl = new URL("http://domain.com/path/");
        String derivedUrl = UrlUtils.makeUrl(baseUrl, "./file.html");
        assertEquals("http://domain.com/path/file.html", derivedUrl);
    }

    @Test
    public void testRawFile() throws MalformedURLException {
        URL baseUrl;
        String derivedUrl;

        baseUrl = new URL("http://domain.com/path/");
        derivedUrl = UrlUtils.makeUrl(baseUrl, "file.html");
        assertEquals("http://domain.com/path/file.html", derivedUrl);
    }

    @Test
    public void testRawQuery() throws MalformedURLException {
        URL baseUrl;
        String derivedUrl;

        // See
        // http://www.communities.hp.com/securitysoftware/blogs/jeff/archive/2007/12/19/RFC-1808-vs-2396-vs-3986_3A00_-Browsers-vs.-programing-languages.aspx
        // Also http://www.ietf.org/rfc/rfc3986.txt
        // Also http://issues.apache.org/jira/browse/NUTCH-566
        // Also http://issues.apache.org/jira/browse/NUTCH-436
        baseUrl = new URL("http://domain.com/path/");
        derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
        assertEquals("http://domain.com/path/?pid=1", derivedUrl);

        baseUrl = new URL("http://domain.com/file");
        derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
        assertEquals("http://domain.com/file?pid=1", derivedUrl);

        baseUrl = new URL("http://domain.com/path/d;p?q#f");
        derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
        assertEquals("http://domain.com/path/d;p?pid=1", derivedUrl);
    }

    @Test
    public void testAbsoluteUrl() throws MalformedURLException {
        URL baseUrl;

        baseUrl = new URL("http://domain.com/path/to/file");
        String derivedUrl = UrlUtils.makeUrl(baseUrl, "http://domain2.com/newpath");
        assertEquals("http://domain2.com/newpath", derivedUrl);
    }

    @Test
    public void testProtocolAndDomain() throws MalformedURLException {
        assertEquals("http://domain.com", UrlUtils.makeProtocolAndDomain("http://domain.com/index.html"));
        assertEquals("http://domain.com", UrlUtils.makeProtocolAndDomain("http://domain.com:80/index.html"));
        assertEquals("http://domain.com:8080", UrlUtils.makeProtocolAndDomain("http://domain.com:8080/index.html"));
        assertEquals("https://domain.com", UrlUtils.makeProtocolAndDomain("https://domain.com/"));
        
        try {
            UrlUtils.makeProtocolAndDomain("mailto:name@domain.com");
            fail("Exception should be thrown");
        } catch (MalformedURLException e) {
            
        }
    }
}
