package bixo.utils;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;


public class UrlUtilsTest {
	
	@Test
	public void testQueryParamReplacingQueryParam() throws MalformedURLException {
		URL baseUrl = new URL("http://domain.com?p=1");
		URL derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=2");
		Assert.assertEquals("http://domain.com/?pid=2", derivedUrl.toExternalForm());
	}
	
	@Test
	public void testQueryParamCase() throws MalformedURLException {
		URL baseUrl = new URL("http://domain.com");
		URL derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
		Assert.assertEquals("http://domain.com/?pid=1", derivedUrl.toExternalForm());
	}

	@Test
	public void testLeadingSlash() throws MalformedURLException {
		URL baseUrl;
		URL derivedUrl;
		
		baseUrl = new URL("http://domain.com/path/to/file");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "/file.html");
		Assert.assertEquals("http://domain.com/file.html", derivedUrl.toExternalForm());
	}
	
	@Test
	public void testLeadingDotSlash() throws MalformedURLException {
		URL baseUrl;
		URL derivedUrl;

		baseUrl = new URL("http://domain.com/path/");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "./file.html");
		Assert.assertEquals("http://domain.com/path/file.html", derivedUrl.toExternalForm());
	}
	
	@Test
	public void testRawFile() throws MalformedURLException {
		URL baseUrl;
		URL derivedUrl;

		baseUrl = new URL("http://domain.com/path/");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "file.html");
		Assert.assertEquals("http://domain.com/path/file.html", derivedUrl.toExternalForm());
	}
	
	@Test
	public void testRawQuery() throws MalformedURLException {
		URL baseUrl;
		URL derivedUrl;

		// See http://www.communities.hp.com/securitysoftware/blogs/jeff/archive/2007/12/19/RFC-1808-vs-2396-vs-3986_3A00_-Browsers-vs.-programing-languages.aspx
		// Also http://www.ietf.org/rfc/rfc3986.txt
		// Also http://issues.apache.org/jira/browse/NUTCH-566
		// Also http://issues.apache.org/jira/browse/NUTCH-436
		baseUrl = new URL("http://domain.com/path/");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
		Assert.assertEquals("http://domain.com/path/?pid=1", derivedUrl.toExternalForm());
		
		baseUrl = new URL("http://domain.com/file");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
		Assert.assertEquals("http://domain.com/file?pid=1", derivedUrl.toExternalForm());

		baseUrl = new URL("http://domain.com/path/d;p?q#f");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "?pid=1");
		Assert.assertEquals("http://domain.com/path/d;p?pid=1", derivedUrl.toExternalForm());
	}
	
	@Test
	public void testAbsoluteUrl() throws MalformedURLException {
		URL baseUrl;
		URL derivedUrl;
		
		baseUrl = new URL("http://domain.com/path/to/file");
		derivedUrl = UrlUtils.makeUrl(baseUrl, "http://domain2.com/newpath");
		Assert.assertEquals("http://domain2.com/newpath", derivedUrl.toExternalForm());
	}

}
