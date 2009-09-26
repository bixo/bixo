package bixo.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.fetcher.http.IHttpHeaders;


public class SimpleParserTest {

	@Test
	public void testRelativeLinkWithBaseUrl() throws IOException {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/base-url.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/base-url.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify outlink is correct.
		Outlink[] outlinks = parsedDatum.getOutlinks();
		Assert.assertEquals(2, outlinks.length);
		
		// TODO KKr - reenable this test when Tika parser calls my handler with
		// the <base> element, which is needed to correctly resolve relative links.
		// Assert.assertEquals("http://newdomain.com/link", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testRelativeLinkWithLocationUrl() throws IOException {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/relative-urls.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/relative-urls.html";
		String location = "http://newdomain.com";
		
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		headers.add(IHttpHeaders.CONTENT_LOCATION, location);
		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify outlink is correct.
		Outlink[] outlinks = parsedDatum.getOutlinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://newdomain.com/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testRelativeLinkWithRelativeLocationUrl() throws IOException {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/relative-urls.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/relative-urls.html";
		String location = "redirected/";
		
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		headers.add(IHttpHeaders.CONTENT_LOCATION, location);
		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify outlink is correct.
		Outlink[] outlinks = parsedDatum.getOutlinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://olddomain.com/redirected/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testRelativeLinkWithRedirectUrl() throws IOException {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/relative-urls.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/relative-urls.html";
		String redirectedUrl = "http://newdomain.com";
		
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, redirectedUrl, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify outlink is correct.
		Outlink[] outlinks = parsedDatum.getOutlinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://newdomain.com/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testContentExtraction() throws IOException {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/simple-content.html");
		
		// Create FetchedDatum using data
		String url = "http://domain.com/simple-content.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify content is correct
		Assert.assertEquals("Simple", parsedDatum.getTitle());
		
		// TODO KKr - Fix up test when Tika HtmlParser stops returning text that
		// follows the </body> tag as though it were before the tag. Currently
		// we get back \n\nContent\n\n\n\n, where the last two returns are the
		// ones after the </body> and </html> tags.
		Assert.assertEquals(encodeReturns("\n\nContent\n\n\n\n"), encodeReturns(parsedDatum.getParsedText()));
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String, Comparable> makeMetadata() {
		return new HashMap<String, Comparable>();
	}
	private static String readFromFile(String filePath) throws IOException {
		InputStream is = SimpleParserTest.class.getResourceAsStream("/" + filePath);
		
		return IOUtils.toString(is);
	}
	
	private static String encodeReturns(String str) throws IOException {
		StringBuilder result = new StringBuilder();
		
		int numLines = 0;
		StringReader sr = new StringReader(str);
		BufferedReader br = new BufferedReader(sr);
		for (String line = br.readLine(); line != null; numLines++, line = br.readLine()) {
			result.append(line);
			result.append("\\n");
		}
		
		// If our last line didn't actually have a return, get rid of it.
		if (!str.endsWith("\n") && (numLines > 0)) {
			result.setLength(result.length() - 2);
		}
		
		return result.toString();
	}
}
