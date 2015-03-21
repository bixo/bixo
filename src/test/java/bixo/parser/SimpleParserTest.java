/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.parser;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.ccil.cowan.tagsoup.Parser;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.dom4j.XPath;
import org.dom4j.io.SAXReader;
import org.junit.Test;

import bixo.config.ParserPolicy;
import bixo.datum.ContentBytes;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.fetcher.HttpHeaderNames;


public class SimpleParserTest {

	@Test
	public void testRelativeLinkWithBaseUrl() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/base-url.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/base-url.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
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
	public void testRelativeLinkWithLocationUrl() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/relative-urls.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/relative-urls.html";
		String location = "http://newdomain.com";
		
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		headers.add(HttpHeaderNames.CONTENT_LOCATION, location);
		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify outlink is correct.
		Outlink[] outlinks = parsedDatum.getOutlinks();
		Assert.assertEquals(2, outlinks.length);
		
		Assert.assertEquals("http://newdomain.com/link1", outlinks[0].getToUrl());
		Assert.assertEquals("link1", outlinks[0].getAnchor());
        // TODO KKr - reenable this test when Tika changes are submitted:
		// Assert.assertEquals("nofollow", outlinks[0].getRelAttributes());
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
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		headers.add(HttpHeaderNames.CONTENT_LOCATION, location);
		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
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
	public void testRelativeLinkWithRedirectUrl() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/relative-urls.html");
		
		// Create FetchedDatum using data
		String url = "http://olddomain.com/relative-urls.html";
		String redirectedUrl = "http://newdomain.com";
		
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, redirectedUrl, System.currentTimeMillis(), headers, content, contentType, 0);
		
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
    public void testDefaultLinkTypes() throws Exception {
        // Read in test data from test/resources
        String html = readFromFile("parser-files/all-link-types.html");
        
        // Create FetchedDatum using data
        String url = "http://domain.com/all-link-types.html";
        
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        // Call parser.parse
        SimpleParser parser = new SimpleParser();
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Verify outlinks are correct (and we only get the a href ones).
        Outlink[] outlinks = parsedDatum.getOutlinks();
        Assert.assertEquals(2, outlinks.length);
        
        Assert.assertEquals("http://newdomain.com/link1", outlinks[0].getToUrl());
        Assert.assertEquals("link1", outlinks[0].getAnchor());
        Assert.assertEquals("http://domain.com/link2", outlinks[1].getToUrl());
        Assert.assertEquals("link2", outlinks[1].getAnchor());
    }
    
    @Test
    public void testAllLinkTypes() throws Exception {
        // Read in test data from test/resources
        String html = readFromFile("parser-files/all-link-types.html");
        
        // Create FetchedDatum using data
        String url = "http://domain.com/all-link-types.html";
        
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        // Call parser.parse
        ParserPolicy policy = new ParserPolicy( ParserPolicy.DEFAULT_MAX_PARSE_DURATION,
                                                BaseLinkExtractor.ALL_LINK_TAGS,
                                                BaseLinkExtractor.ALL_LINK_ATTRIBUTE_TYPES);
        SimpleParser parser = new SimpleParser(policy);
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Verify outlinks are correct (and we only get the a href ones).
        Outlink[] outlinks = parsedDatum.getOutlinks();
        Assert.assertEquals(7, outlinks.length);
        
        Assert.assertEquals("http://newdomain.com/favicon.ico", outlinks[0].getToUrl());
        Assert.assertEquals("http://newdomain.com/link1", outlinks[1].getToUrl());
        Assert.assertEquals("link1", outlinks[1].getAnchor());
        Assert.assertEquals("http://domain.com/link2", outlinks[2].getToUrl());
        Assert.assertEquals("link2", outlinks[2].getAnchor());
        Assert.assertEquals("http://newdomain.com/giant-prawn.jpg", outlinks[3].getToUrl());
        Assert.assertEquals("http://en.wikipedia.org/wiki/Australia's_Big_Things",
                            outlinks[4].getToUrl());
        Assert.assertEquals("http://newdomain.com/giant-dog.jpg", outlinks[5].getToUrl());
        Assert.assertEquals("http://www.brucelawson.co.uk/index.php/2005/stupid-stock-photography/",
                            outlinks[6].getToUrl());
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testSomeLinkTypes() throws Exception {
        // Read in test data from test/resources
        String html = readFromFile("parser-files/all-link-types.html");
        
        // Create FetchedDatum using data
        String url = "http://domain.com/all-link-types.html";
        
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        // Call parser.parse
        Set<String> linkTags =
            new HashSet<String>() {{
                add("a");
                add("img");
                add("link");
            }};
            
        Set<String> linkAttributeTypes =
            new HashSet<String>() {{
                add("href");
                add("src");
            }};

        ParserPolicy policy = new ParserPolicy( ParserPolicy.DEFAULT_MAX_PARSE_DURATION,
                                                linkTags,
                                                linkAttributeTypes);
        SimpleParser parser = new SimpleParser(policy);
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Verify outlinks are correct (and we only get the a href ones).
        Outlink[] outlinks = parsedDatum.getOutlinks();
        Assert.assertEquals(4, outlinks.length);
        
        Assert.assertEquals("http://newdomain.com/favicon.ico", outlinks[0].getToUrl());
        Assert.assertEquals("http://newdomain.com/link1", outlinks[1].getToUrl());
        Assert.assertEquals("link1", outlinks[1].getAnchor());
        Assert.assertEquals("http://domain.com/link2", outlinks[2].getToUrl());
        Assert.assertEquals("link2", outlinks[2].getAnchor());
        Assert.assertEquals("http://newdomain.com/giant-prawn.jpg", outlinks[3].getToUrl());
    }
    
	@Test
	public void testContentExtraction() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/simple-content.html");
		
		// Create FetchedDatum using data
		String url = "http://domain.com/simple-content.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify content is correct
		Assert.assertEquals("Simple", parsedDatum.getTitle());
		
		compareTermsInStrings("Simple Content", parsedDatum.getParsedText());
	}
	
    @Test
    public void testHtmlParsing() throws Exception {
        URL path = SimpleParserTest.class.getResource("/simple-page.html");

        BaseParser parser = new SimpleParser();
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

    @SuppressWarnings("serial")
    @Test
    public void testCustomContentExtractor() throws Exception {
        String html = readFromFile("parser-files/simple-content.html");
        
        String url = "http://domain.com/simple-content.html";
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        SimpleParser parser = new SimpleParser(new BaseContentExtractor() {

            @Override
            public String getContent() {
                return "Custom";
            }
        }, 
        new BaseLinkExtractor() {
            
            @Override
            public Outlink[] getLinks() {
                return new Outlink[0];
            }
        },
        new ParserPolicy());
        
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Verify content is correct
        Assert.assertEquals("Simple", parsedDatum.getTitle());
        
        compareTermsInStrings("Custom", parsedDatum.getParsedText());
    }
    
    @Test
    public void testLinkExtractorWithMetaTags() throws Exception {
        String html = readFromFile("parser-files/meta-nofollow.html");
        
        String url = "http://domain.com/meta-nofollow.html";
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        ParserPolicy policy = new ParserPolicy(Integer.MAX_VALUE);
        SimpleParser parser = new SimpleParser(policy);
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Verify we got no URLs
        Assert.assertEquals(0, parsedDatum.getOutlinks().length);
    }
    
    @Test
    public void testLanguageDetectionHttpHeader() throws Exception {
		// Read in test data from test/resources
		String html = readFromFile("parser-files/simple-content.html");
		
		// Create FetchedDatum using data
		String url = "http://domain.com/simple-content.html";
		String contentType = "text/html; charset=utf-8";
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		headers.add(HttpHeaderNames.CONTENT_LANGUAGE, "en");

		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
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
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		headers.add(HttpHeaderNames.CONTENT_LANGUAGE, "en");

		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
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
		headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
		headers.add(HttpHeaderNames.CONTENT_LANGUAGE, "en");

		ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
		FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
		
		// Call parser.parse
		SimpleParser parser = new SimpleParser();
		ParsedDatum parsedDatum = parser.parse(fetchedDatum);
		
		// Verify content is correct
		Assert.assertEquals("SimpleHttpEquiv", parsedDatum.getTitle());
		
		compareTermsInStrings("SimpleHttpEquiv Content", parsedDatum.getParsedText());
		
		// In Tika 1.5 or later, the HTTP header has priority over what's in any <meta http-equiv="Content-Language" xxx> data
		Assert.assertEquals("en", parsedDatum.getLanguage());

    }

    @Test
    public void testExtractingObjectTag() throws Exception {
        final String html = "<html><head><title>Title</title></head>" +
            "<body><object data=\"http://domain.com/song.mid\" /></body></html>";
        
        // Create FetchedDatum using data
        String url = "http://domain.com/music.html";
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(html.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        // Call parser.parse
        ParserPolicy policy = new ParserPolicy( ParserPolicy.NO_MAX_PARSE_DURATION,
                                                BaseLinkExtractor.ALL_LINK_TAGS,
                                                BaseLinkExtractor.ALL_LINK_ATTRIBUTE_TYPES);
        SimpleParser parser = new SimpleParser(new SimpleContentExtractor(), new SimpleLinkExtractor(), policy, true);
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Verify outlinks are correct
        Outlink[] outlinks = parsedDatum.getOutlinks();
        Assert.assertEquals(1, outlinks.length);
        Assert.assertEquals("http://domain.com/song.mid", outlinks[0].getToUrl());
    }
    
    @Test
    public void testHtmlWithTags() throws Exception {
        final String htmlText = "<html><head><title>Title</title></head>" +
                        "<body><p>this is a test</p></body></html>";
        
        // Create FetchedDatum using data
        String url = "http://domain.com/page.html";
        String contentType = "text/html; charset=utf-8";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, contentType);
        ContentBytes content = new ContentBytes(htmlText.getBytes("utf-8"));
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), headers, content, contentType, 0);
        
        // Call parser.parse
        SimpleParser parser = new SimpleParser(new ParserPolicy(), true);
        ParsedDatum parsedDatum = parser.parse(fetchedDatum);
        
        // Now take the resulting HTML, process it using Dom4J
        SAXReader reader = new SAXReader(new Parser());
        reader.setEncoding("UTF-8");
        String htmlWithMarkup = parsedDatum.getParsedText();
        Document doc = reader.read(new ByteArrayInputStream(htmlWithMarkup.getBytes("UTF-8")));
        
        // We have to do helicopter stunts since HTML has a global namespace on it, set
        // at the <html> element level.
        XPath xpath = DocumentHelper.createXPath("/xhtml:html/xhtml:body/xhtml:p");
        Map<String, String> namespaceUris = new HashMap<String, String>();
        namespaceUris.put("xhtml", "http://www.w3.org/1999/xhtml");
        xpath.setNamespaceURIs(namespaceUris);
        
        Node paragraphNode = xpath.selectSingleNode(doc);
        Assert.assertNotNull(paragraphNode);
        Assert.assertEquals("this is a test", paragraphNode.getText());
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
        in.close();
        String url = path.toExternalForm().toString();
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), new HttpHeaders(), new ContentBytes(bytes), "text/html", 0);
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
