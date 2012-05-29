package bixo.examples.crawl;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/26/12
 * Time: 9:40 AM
 * To change this template use File | Settings | File Templates.
 */

import bixo.parser.BaseContentExtractor;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * BoilerpipeContentExtractor is a content extractor that extracts Boilerpipe cleaned content
 * written by Ken Krugler KKrugler_lists@transpac.com
 *
 */

@SuppressWarnings("serial")
public class BoilerpipeContentExtractor extends BaseContentExtractor {


    private transient BoilerpipeContentHandler _bpContentHandler;

    public BoilerpipeContentExtractor() {
        init();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri)
            throws SAXException {
        _bpContentHandler.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        _bpContentHandler.endPrefixMapping(prefix);
    }


    @Override
    public void processingInstruction(String target, String data)
            throws SAXException {
        _bpContentHandler.processingInstruction(target, data);
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        _bpContentHandler.setDocumentLocator(locator);
    }

    @Override
    public void startDocument() throws SAXException {
        init();

        _bpContentHandler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
        _bpContentHandler.endDocument();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        _bpContentHandler.startElement(uri, localName, qName, atts);
        if (localName.equalsIgnoreCase("script")) {
            System.out.println("we shouldn't get script tags");
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        _bpContentHandler.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        _bpContentHandler.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length)
            throws SAXException {
        _bpContentHandler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        _bpContentHandler.skippedEntity(name);
    }

    /**
     * getContent returns the boilerpipe extracted text.
     */
    @Override
    public String getContent() {
        TextDocument textDocument = _bpContentHandler.toTextDocument();
        return textDocument.getText(true, false);
    }

    @Override
    public void reset() {

        // Unfortunately there's no good way to reset the BoilerpipeContentHandler,
        // so we have to force it to be recreated
        _bpContentHandler = null;
        init();
    }


    protected synchronized void init() {

        if (_bpContentHandler == null) {
            BodyContentHandler bodyContentHandler = new BodyContentHandler();
            _bpContentHandler = new BoilerpipeContentHandler(bodyContentHandler, DefaultExtractor.INSTANCE);
        }
    }


}