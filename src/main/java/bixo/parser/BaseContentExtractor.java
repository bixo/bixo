package bixo.parser;

import java.io.Serializable;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

@SuppressWarnings("serial")
public abstract class BaseContentExtractor extends DefaultHandler implements Serializable {

    protected boolean _inHead;
    protected boolean _inBody;
    protected boolean _inTitle;

    public void reset() {
        _inHead = false;
        _inBody = false;
        _inTitle = false;
    }
    
    public abstract String getContent();
    
    // Routines to simplify using base functionality.
    public void addContent(char[] ch, int start, int length) {};
    public void addContent(char ch) {};
    
    @Override
    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        super.startElement(uri, localName, name, atts);

        if (_inHead) {
            if (localName.equalsIgnoreCase("title")) {
                _inTitle = true;
            }
        } else if (localName.equalsIgnoreCase("head")) {
            _inHead = true;
        } else if (localName.equalsIgnoreCase("body")) {
            _inBody = true;
        }
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        
        if (_inTitle) {
            addContent(ch, start, length);
            addContent(' ');
        } else if (_inBody) {
            addContent(ch, start, length);
        }
    }
    
    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);

        if (_inHead && localName.equalsIgnoreCase("head")) {
            _inHead = false;
        } else if (_inTitle && localName.equalsIgnoreCase("title")) {
            _inTitle = false;
        } else if (_inBody && localName.equalsIgnoreCase("body")) {
            _inBody = false;
        }
    }

}
