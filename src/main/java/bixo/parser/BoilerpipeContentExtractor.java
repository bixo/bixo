package bixo.parser;

import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.extractors.ExtractorBase;

/**
 * BoilerpipeContentExtractor is a content extractor that extracts Boilerpipe cleaned content
 *
 */
@SuppressWarnings("serial")
public class BoilerpipeContentExtractor extends BaseContentExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BoilerpipeContentExtractor.class);
    
    
    private Class<? extends ExtractorBase> _extractorClass;
    private transient BoilerpipeContentHandler _bpContentHandler;
    
    /**
     * Defaults to using {@link DefaultExtractor} when setting up 
     * the {@link BoilerpipeContentHandler}
     */
    public BoilerpipeContentExtractor() {
        this(DefaultExtractor.class);
    }

    /**
     * {@link BoilerpipeExtractor} doesn't implement Serializable, but a caller can work around 
     * this limitation by specifying the BoilerpipeExtractor class to use with 
     * the {@link BoilerpipeContentHandler} (this would work for most extractors; 
     * it won't work for {@link KeepEverythingWithMinKWordsExtractor} which takes a parameter). 
     */
    public BoilerpipeContentExtractor(Class<? extends ExtractorBase> extractorClass) {
        _extractorClass = extractorClass;
    }

    private BoilerpipeExtractor initExtractor(Class<? extends ExtractorBase> extractorClass) {
        BoilerpipeExtractor extractor = null;
        try {
            extractor = (BoilerpipeExtractor) extractorClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException (e.getMessage());            
        }
        return extractor;
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
            LOGGER.warn("we shouldn't get script tags when using Boilerpipe");
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
        TextDocument textDocument = _bpContentHandler.getTextDocument();
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
            BoilerpipeExtractor extractor = initExtractor(_extractorClass);
            BodyContentHandler bodyContentHandler = new BodyContentHandler();
            _bpContentHandler = new BoilerpipeContentHandler(bodyContentHandler, extractor);
        }
    }
}
