package bixo.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
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
        private boolean _inTitle = false;
        
        private String _curUrl;
        private StringBuilder _curAnchor = new StringBuilder();
        
        public LinkBodyHandler() {
        	_baseUrl = null;
        }
        
        public LinkBodyHandler(URL baseUrl) {
            _baseUrl = baseUrl;
        }
        
        public void setBaseUrl(URL baseUrl) {
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
            			// Handle relative URLs, even though by definition it should be absolute.
            			_baseUrl = new URL(UrlUtils.makeUrl(_baseUrl, attributes.getValue("href").trim()));
            		} catch (MalformedURLException e) {
                        LOGGER.debug("Invalid URL found in <base> tag: ", e);
            		}
            	} else if (localName.equalsIgnoreCase("title")) {
            		_inTitle = true;
            	}
            } else if (_inBody) {
            	if (localName.equalsIgnoreCase("a")) {
                    try {
                        _curUrl = UrlUtils.makeUrl(_baseUrl, attributes.getValue("href").trim());
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
        	super.endElement(uri, localName, name);
        	
        	if (_inHead && localName.equalsIgnoreCase("head")) {
        		_inHead = false;
        	} else if (_inTitle && localName.equalsIgnoreCase("title")) {
        		_inTitle = false;
        	} else if (_inBody && localName.equalsIgnoreCase("body")) {
        		_inBody = false;
        	} else if (_inAnchor && localName.equalsIgnoreCase("a")) {
                _outlinks.add(new Outlink(_curUrl, _curAnchor.toString()));
                _inAnchor = false;
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
        	super.characters(ch, start, length);
        	
        	if (_inAnchor) {
                _curAnchor.append(ch, start, length);
                _content.append(ch, start, length);
        	} else if (_inTitle) {
        		_content.append(ch, start, length);
        		_content.append(' ');
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

        LinkBodyHandler handler = new LinkBodyHandler();

        try {
        	URL baseUrl = new URL(fetchedDatum.getFetchedUrl());
        	
        	// See if we have a content location from the HTTP headers that we should use as
        	// the base for resolving relative URLs in the document.
        	String clUrl = fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_LOCATION);
        	if (clUrl != null) {
        		// FUTURE KKr - should we try to keep processing if this step fails, but
        		// refuse to resolve relative links?
        		baseUrl = new URL(baseUrl, clUrl);
        	}
        	
        	handler.setBaseUrl(baseUrl);

            _parser.parse(is, handler, metadata);
            
            return new ParsedDatum(fetchedDatum.getBaseUrl(), handler.getContent(), metadata.get(Metadata.TITLE),
                            handler.getLinks(), fetchedDatum.getMetaDataMap());
        } catch (MalformedURLException e) {
            // TODO KKr - throw exception once ParseFunction handles this.
            LOGGER.warn("Exception processing document URLs for " + fetchedDatum.getBaseUrl(), e);
            return null;
        } catch (TikaException e) {
            LOGGER.warn("Exception parsing document " + fetchedDatum.getBaseUrl(), e);
            if (e.getCause() instanceof SAXParseException) {
            	// TODO KKr - remove this when Tika bugs w/using XML parser on HTML, and failing
            	// on RSS, are fixed.
                return new ParsedDatum(fetchedDatum.getBaseUrl(), handler.getContent(), metadata.get(Metadata.TITLE),
                        handler.getLinks(), fetchedDatum.getMetaDataMap());
            } else {
            	// TODO KKr - throw exception once ParseFunction handles this.
            	return null;
            }
        } catch (Exception e) {
            // TODO KKr - throw exception once ParseFunction handles this.
            LOGGER.warn("Exception parsing document " + fetchedDatum.getBaseUrl(), e);
            return null;
        } finally {
            IOUtils.safeClose(is);
        }
    }

}
