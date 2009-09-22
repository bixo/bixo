package org.apache.tika.parser.mbox;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

public class MboxParserTest extends TestCase {

	public void testSimple() {
		Parser parser = new MboxParser();
        Metadata metadata = new Metadata();
        InputStream stream = getStream("test-documents/simple.mbox");
        ContentHandler handler = mock(DefaultHandler.class);
        
        try {
        	parser.parse(stream, handler, metadata);
            verify(handler).startDocument();
            verify(handler).startElement(eq(XHTMLContentHandler.XHTML), eq("p"), eq("p"), any(Attributes.class));
            verify(handler).characters(new String("Test content").toCharArray(), 0, 12);
            verify(handler).endDocument();
        } catch (Exception e) {
        	fail("Exception thrown: " + e.getMessage());
        }
	}
	
	public void testHeaders() {
		Parser parser = new MboxParser();
        Metadata metadata = new Metadata();
        InputStream stream = getStream("test-documents/headers.mbox");
        ContentHandler handler = mock(DefaultHandler.class);

        try {
        	parser.parse(stream, handler, metadata);

            verify(handler).startDocument();
            verify(handler).startElement(eq(XHTMLContentHandler.XHTML), eq("p"), eq("p"), any(Attributes.class));
            verify(handler).characters(new String("Test content").toCharArray(), 0, 12);
            verify(handler).endDocument();

            assertEquals("subject", metadata.get(Metadata.TITLE));
        	assertEquals("subject", metadata.get(Metadata.SUBJECT));
        	assertEquals("<author@domain.com>", metadata.get(Metadata.AUTHOR));
        	assertEquals("<author@domain.com>", metadata.get(Metadata.CREATOR));
        	assertEquals("<name@domain.com>", metadata.get("MboxParser-return-path"));
        	assertEquals("Tue, 9 Jun 2009 23:58:45 -0400", metadata.get(Metadata.DATE));
        } catch (Exception e) {
        	fail("Exception thrown: " + e.getMessage());
        }
	}
	
	public void testMultilineHeader() {
		Parser parser = new MboxParser();
        Metadata metadata = new Metadata();
        InputStream stream = getStream("test-documents/multiline.mbox");
        ContentHandler handler = mock(DefaultHandler.class);

        try {
        	parser.parse(stream, handler, metadata);

            verify(handler).startDocument();
            verify(handler).startElement(eq(XHTMLContentHandler.XHTML), eq("p"), eq("p"), any(Attributes.class));
            verify(handler).characters(new String("Test content").toCharArray(), 0, 12);
            verify(handler).endDocument();

        	assertEquals("from xxx by xxx with xxx; date", metadata.get("MboxParser-received"));
        } catch (Exception e) {
        	fail("Exception thrown: " + e.getMessage());
        }
	}
	
	public void testQuoted() {
		Parser parser = new MboxParser();
        Metadata metadata = new Metadata();
        InputStream stream = getStream("test-documents/quoted.mbox");
        ContentHandler handler = mock(DefaultHandler.class);

        try {
        	parser.parse(stream, handler, metadata);

            verify(handler).startDocument();
            verify(handler).startElement(eq(XHTMLContentHandler.XHTML), eq("p"), eq("p"), any(Attributes.class));
            verify(handler).startElement(eq(XHTMLContentHandler.XHTML), eq("q"), eq("q"), any(Attributes.class));
            verify(handler).endElement(eq(XHTMLContentHandler.XHTML), eq("q"), eq("q"));
            verify(handler).endElement(eq(XHTMLContentHandler.XHTML), eq("p"), eq("p"));
            verify(handler).characters(new String("Test content").toCharArray(), 0, 12);
            verify(handler).characters(new String("> quoted stuff").toCharArray(), 0, 14);
            verify(handler).endDocument();
        } catch (Exception e) {
        	fail("Exception thrown: " + e.getMessage());
        }
	}
	
    private static InputStream getStream(String name) {
        return Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(name);
    }


}
