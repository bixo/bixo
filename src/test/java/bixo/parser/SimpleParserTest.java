package bixo.parser;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
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
	public void testRelativeLinkWithBaseUrl() throws Exception {
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
		Outlink[] outlinks = parsedDatum.getOutLinks();
		Assert.assertEquals(2, outlinks.length);
		
		// TODO KKr - reenable this test when Tika parser calls my handler with
		// the <base> element, which is needed to correctly resolve relative links.
		// Assert.assertEquals("http://newdomain.com/link", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testRelativeLinkWithLocationUrl() throws Exception {
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
		Outlink[] outlinks = parsedDatum.getOutLinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://newdomain.com/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testRelativeLinkWithRelativeLocationUrl() throws Exception {
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
		Outlink[] outlinks = parsedDatum.getOutLinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://olddomain.com/redirected/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testRelativeLinkWithRedirectUrl() throws Exception {
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
		Outlink[] outlinks = parsedDatum.getOutLinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://newdomain.com/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
		Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
		Assert.assertEquals("link2", outlinks[1].getAnchor());
	}
	
	@Test
	public void testContentExtraction() throws Exception {
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
		
		compareTermsInStrings("Simple Content", parsedDatum.getParsedText());
	}
	
    @Test
    public void testHtlmParsing() throws Exception {
        URL path = SimpleParserTest.class.getResource("/" + "simple-page.html");

        IParser parser = new SimpleParser();
        FetchedDatum content = makeFetchedDatum(path);
        ParsedDatum parse = parser.parse(content);
        Assert.assertNotNull(parse.getParsedText());
        
        // TODO - add back in title text to simple-page, when we generate this
        File parsedTextFile = new File(SimpleParserTest.class.getResource("/" + "simple-page.txt").getFile());
        String expectedString = FileUtils.readFileToString(parsedTextFile, "utf-8");
        String actualString = parse.getParsedText();
        
        // Trim of leading returns so split() doesn't give us an empty term
        // TODO - use our own split that skips leading/trailing separators
        compareTermsInStrings(expectedString, actualString.replaceFirst("^[\\n]+", ""));

        // TODO reenable when Tika bug is fixed re not emitting <img> links.
        // Outlink[] outlinks = parse.getOutlinks();
        // Assert.assertEquals(10, outlinks.length);
        
        Assert.assertEquals("TransPac Software", parse.getTitle());
    }

    @Test
    public void testLanguageDetectionHttpHeader() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/simple-content.html");
		
		// Create FetchedDatum using data
		String url = "http://domain.com/simple-content.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		headers.add(IHttpHeaders.CONTENT_LANGUAGE, "en");

		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify content is correct
		Assert.assertEquals("Simple", parsedDatum.getTitle());
		
		compareTermsInStrings("Simple Content", parsedDatum.getParsedText());
		Assert.assertEquals("en", parsedDatum.getLanguage());

    }
    
    @Test
    public void testLanguageDetectionDublinCore() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/lang-dc.html");
		
		// Create FetchedDatum using data
		String url = "http://domain.com/lang-dc.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		headers.add(IHttpHeaders.CONTENT_LANGUAGE, "en");

		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify content is correct
		Assert.assertEquals("DublinCore Language Example", parsedDatum.getTitle());
		
		compareTermsInStrings("DublinCore Language Example Content", parsedDatum.getParsedText());
		
		Assert.assertEquals("ja", parsedDatum.getLanguage());

    }

    @Test
    public void testLanguageDetectionHttpEquiv() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/lang-http-equiv.html");
		
		// Create FetchedDatum using data
		String url = "http://domain.com/lang-dc.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(IHttpHeaders.CONTENT_TYPE, contentType);
		headers.add(IHttpHeaders.CONTENT_ENCODING, "utf-8");
		headers.add(IHttpHeaders.CONTENT_LANGUAGE, "en");

		BytesWritable content = new BytesWritable(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0, makeMetadata());
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify content is correct
		Assert.assertEquals("SimpleHttpEquiv", parsedDatum.getTitle());
		
		compareTermsInStrings("SimpleHttpEquiv Content", parsedDatum.getParsedText());
		
		Assert.assertEquals("ja", parsedDatum.getLanguage());

    }

	@SuppressWarnings("unchecked")
	private static Map<String, Comparable> makeMetadata() {
		return new HashMap<String, Comparable>();
	}
	private static String readFromFile(String filePath) throws IOException {
		InputStream is = SimpleParserTest.class.getResourceAsStream("/" + filePath);
		
		return IOUtils.toString(is);
	}
	
    private FetchedDatum makeFetchedDatum(URL path) throws IOException {
        File file = new File(path.getFile());
        byte[] bytes = new byte[(int) file.length()];
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        in.readFully(bytes);

        String url = path.toExternalForm().toString();
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), new HttpHeaders(), new BytesWritable(bytes), "text/html", 0, null);
        return fetchedDatum;
    }
    
    private void compareTermsInStrings(String expected, String actual) {
        String[] expectedTerms = expected.split("[ \\n\\r\\t\\n]+");
        // Trim of leading returns so split() doesn't give us an empty term
        // TODO - use our own split that skips leading/trailing separators
        String[] actualTerms = actual.split("[ \\n\\r\\t\\n]+");
        
        int compLength = Math.min(expectedTerms.length, actualTerms.length);
        for (int i = 0; i < compLength; i++) {
        	Assert.assertEquals("Term at index " + i, expectedTerms[i], actualTerms[i]);
        }
        
        Assert.assertEquals(expectedTerms.length, actualTerms.length);
    }


}
