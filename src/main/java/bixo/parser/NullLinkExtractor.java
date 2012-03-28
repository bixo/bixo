package bixo.parser;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import bixo.datum.Outlink;

@SuppressWarnings("serial")
public class NullLinkExtractor extends BaseLinkExtractor {

    private static final Outlink[] EMPTY_RESULT = new Outlink[0];

    public static final NullLinkExtractor INSTANCE = new NullLinkExtractor();
    
    @Override
    public Outlink[] getLinks() {
        return EMPTY_RESULT;
    }
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        // Do nothing
    }
    
    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        // Do nothing
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        // Do nothing
    }

}
