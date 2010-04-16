package bixo.parser;

import java.io.Serializable;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import bixo.datum.Outlink;

@SuppressWarnings("serial")
public abstract class BaseLinkExtractor extends DefaultHandler implements Serializable {

    protected boolean _inAnchor;        
    protected String _curUrl;
    protected String _curRelAttributes;
    protected StringBuilder _curAnchor = new StringBuilder();

    public void reset() {
        _inAnchor = false;
    }
    
    public void addLink(Outlink link) {};
    
    public abstract Outlink[] getLinks();
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        if (!_inAnchor && localName.equalsIgnoreCase("a")) {
            String hrefAttr = attributes.getValue("href");
            if (hrefAttr != null) {
                _curUrl = hrefAttr;
                _curRelAttributes = attributes.getValue("rel");
                _inAnchor = true;
                _curAnchor.setLength(0);
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        
        if (_inAnchor) {
            _curAnchor.append(ch, start, length);
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);

        if (_inAnchor && localName.equalsIgnoreCase("a")) {
            addLink(new Outlink(_curUrl, _curAnchor.toString(), _curRelAttributes));
            _inAnchor = false;
        }
    }

}
