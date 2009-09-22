package bixo.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.fetcher.http.IHttpHeaders;
import bixo.utils.IOUtils;
import bixo.utils.UrlUtils;

@SuppressWarnings("serial")
public class SimpleParser implements IParser {
    private static final Logger LOGGER = Logger.getLogger(SimpleParser.class);

    private transient AutoDetectParser _parser;
    
    private static class LinkBodyHandler extends DefaultHandler {
        private URL _baseUrl;
        
        private StringBuilder _content = new StringBuilder();
        private List<Outlink> _outlinks = new ArrayList<Outlink>();
        private boolean _inHead = false;
        private boolean _inBody = false;
        private boolean _inAnchor = false;
        
        private String _curUrl;
        private StringBuilder _curAnchor = new StringBuilder();
        
        public LinkBodyHandler(URL baseUrl) {
            _baseUrl = baseUrl;
        }
        
        public String getContent() {
            return _content.toString();
        }
        
        public Outlink[] getLinks() {
            return _outlinks.toArray(new Outlink[_outlinks.size()]);
        }
        
        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            
            if (_inHead) {
            	if (localName.equalsIgnoreCase("base")) {
            		try {
            			_baseUrl = new URL(attributes.getValue("href"));
            		} catch (MalformedURLException e) {
                        LOGGER.debug("Invalid URL found in <base> tag: ", e);
            		}
            	}
            } else if (_inBody) {
            	if (localName.equalsIgnoreCase("a")) {
                    try {
                        _curUrl = UrlUtils.makeUrl(_baseUrl, attributes.getValue("href")).toExternalForm();
                        _inAnchor = true;
                        _curAnchor.setLength(0);
                    } catch (MalformedURLException e) {
                        LOGGER.debug("Invalid URL found in <a> tag: ", e);
                    }
            	}
            } else if (localName.equalsIgnoreCase("head")) {
            	_inHead = true;
            } else if (localName.equalsIgnoreCase("body")) {
            	_inBody = true;
            }
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
        	if (_inHead && localName.equalsIgnoreCase("head")) {
        		_inHead = false;
        	} else if (_inBody && localName.equalsIgnoreCase("body")) {
        		_inBody = false;
        	} else if (_inAnchor && localName.equalsIgnoreCase("a")) {
                _outlinks.add(new Outlink(_curUrl, _curAnchor.toString()));
                _inAnchor = false;
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (_inAnchor) {
                _curAnchor.append(ch, start, length);
            } else if (_inBody) {
                _content.append(ch, start, length);
            }
        }
    };

    private synchronized void init() {
        if (_parser == null) {
            _parser = new AutoDetectParser();
        }
    }

    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) {
        init();

        // Provide clues to the parser about the format of the content.
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, fetchedDatum.getBaseUrl());
        metadata.add(Metadata.CONTENT_TYPE, fetchedDatum.getContentType());

        // TODO KKr - enable this when we have it as part of the FetchedDatum
        // metadata.add(Metadata.CONTENT_ENCODING, fetchedDatum.getContentEncoding());
        metadata.add(Metadata.CONTENT_ENCODING, fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_ENCODING));
        
        InputStream is = new ByteArrayInputStream(fetchedDatum.getContent().getBytes());
        
        try {
            LinkBodyHandler handler = new LinkBodyHandler(new URL(fetchedDatum.getFetchedUrl()));
            _parser.parse(is, handler, metadata);
            
            return new ParsedDatum(fetchedDatum.getBaseUrl(), handler.getContent(), metadata.get(Metadata.TITLE),
                            handler.getLinks(), fetchedDatum.getMetaDataMap());
        } catch (Exception e) {
            // TODO KKr - throw exception once ParseFunction handles this.
            LOGGER.warn("Exception parsing document " + fetchedDatum.getBaseUrl(), e);
            return null;
        } finally {
            IOUtils.safeClose(is);
        }
    }

}
