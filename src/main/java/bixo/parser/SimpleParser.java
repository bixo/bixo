package bixo.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.ProfilingHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.TeeContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.fetcher.http.IHttpHeaders;
import bixo.utils.IoUtils;

@SuppressWarnings("serial")
public class SimpleParser implements IParser {
    private static final Logger LOGGER = Logger.getLogger(SimpleParser.class);

    // Simplistic language code pattern used when there are more than one languages specified
    // FUTURE KKr - improve this to handle en-US, and "eng" for those using old-style language codes.
    private static final Pattern LANGUAGE_CODE_PATTERN= Pattern.compile("([a-z]{2})([,;-]).*");
    private static final Pattern HTTP_EQUIV_CHARSET_PATTERN = Pattern.compile("(?is)<meta\\s+http-equiv\\s*=\\s*['\"]\\s*Content-Type['\"]\\s+content\\s*=\\s*['\"][^;]+;\\s*charset\\s*=\\s*([^'\"]+)\"");

    private transient AutoDetectParser _parser;
    
    private static class LinkBodyHandler extends DefaultHandler {
        private StringBuilder _content = new StringBuilder();
        private List<Outlink> _outlinks = new ArrayList<Outlink>();
        private boolean _inHead = false;
        private boolean _inBody = false;
        private boolean _inAnchor = false;
        private boolean _inTitle = false;
        
        private String _curUrl;
        private StringBuilder _curAnchor = new StringBuilder();
        
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
            	if (localName.equalsIgnoreCase("title")) {
            		_inTitle = true;
            	}
            } else if (_inBody) {
            	if (localName.equalsIgnoreCase("a")) {
            		String hrefAttr = attributes.getValue("href");
            		if (hrefAttr != null) {
            			_curUrl = hrefAttr;
            			_inAnchor = true;
            			_curAnchor.setLength(0);
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

        if (LOGGER.isTraceEnabled()) {
        	LOGGER.trace(String.format("Parsing %s", fetchedDatum.getBaseUrl()));
        }
        
        // Provide clues to the parser about the format of the content.
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, fetchedDatum.getBaseUrl());
        metadata.add(Metadata.CONTENT_TYPE, fetchedDatum.getContentType());
        metadata.add(Metadata.CONTENT_ENCODING, fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_ENCODING));
        String lang = fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_LANGUAGE);
        // TODO KKr - reenable this when TIKA-339 is fixed. We don't want to have anything in metadata
        // before we call the HtmlParser, so that we can detect when the CharsetDetector has incorrectly
        // set both Dublic Core and Http-Equiv languages using charset results.
        // metadata.add(Metadata.CONTENT_LANGUAGE, lang);
        
        // Hack to detect content encoding - this is required due to a bug in Tika since it
        // isn't using the http-equiv charset value.
        String encoding = detectContentEncoding(new String(fetchedDatum.getContentBytes()), metadata);
        if (encoding != null) {
            // Overwrite the content encoding that was set up using the Http headers.
            metadata.set(Metadata.CONTENT_ENCODING, encoding);
        }
        
        InputStream is = new ByteArrayInputStream(fetchedDatum.getContentBytes());

        LinkBodyHandler handler = new LinkBodyHandler();

        try {
        	URL baseUrl = getContentLocation(fetchedDatum);
        	metadata.add(Metadata.CONTENT_LOCATION, baseUrl.toExternalForm());

        	 // Automatic language detection	 
        	ProfilingHandler profilingHandler = new ProfilingHandler();	 
        	
        	
            TeeContentHandler teeContentHandler = new TeeContentHandler(handler, profilingHandler);
            _parser.parse(is, teeContentHandler, metadata);
            
            lang = detectLanguage(lang, metadata, profilingHandler);
            return new ParsedDatum(fetchedDatum.getBaseUrl(), handler.getContent(), lang, metadata.get(Metadata.TITLE),
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
                return new ParsedDatum(fetchedDatum.getBaseUrl(), handler.getContent(), lang, metadata.get(Metadata.TITLE),
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
            IoUtils.safeClose(is);
        }
    }

	private String detectContentEncoding(String content, Metadata metadata) {
		String result = null;
		
        // We are interested in finding if there is an http-equiv defined charset.
        // So this only is needed when dealing with HTML
        String contentType = metadata.get(Metadata.CONTENT_TYPE);
        if (contentType == null) {
            contentType = "";
        } else if (contentType.indexOf(';') != -1) {
            contentType = contentType.substring(0, contentType.indexOf(';')).trim();
        }

        if (contentType.equalsIgnoreCase("text/html")
			|| contentType.equalsIgnoreCase("application/xhtml+xml")
			|| contentType.equalsIgnoreCase("application/x-asp")) {
			Matcher m = HTTP_EQUIV_CHARSET_PATTERN.matcher(content);
			if (m.find()) {
			    result = m.group(1);
				LOGGER.debug("Using encoding specified by http-equiv: " + result);
			}
		}
        
        return result;
	}

	private URL getContentLocation(FetchedDatum fetchedDatum) throws MalformedURLException {
		URL baseUrl = new URL(fetchedDatum.getFetchedUrl());
		
		// See if we have a content location from the HTTP headers that we should use as
		// the base for resolving relative URLs in the document.
		String clUrl = fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_LOCATION);
		if (clUrl != null) {
			// FUTURE KKr - should we try to keep processing if this step fails, but
			// refuse to resolve relative links?
			baseUrl = new URL(baseUrl, clUrl);
		}
		return baseUrl;
	}

	/**
	 * If a language was specified in the HTTP response header, use that.
	 * Otherwise see if a language was set by the CharsetDetector.
	 * As a last resort falls back to the result from the ProfilingHandler.
	 * TODO KKr - remove passing in response lang once TIKA-339 is fixed.
	 *  
	 * @param httpRespLang Language in HTTP response header, or null if no provided
	 * @param metadata
	 * @param profilingHandler
	 * @return The first language found (two char lang code) or empty string if no language was detected.
	 */
	private String detectLanguage(String httpRespLang, Metadata metadata, ProfilingHandler profilingHandler) {
	    String result = null;
	    
	    String dubCoreLang = metadata.get(Metadata.LANGUAGE);
	    String httpEquivLang = metadata.get(Metadata.CONTENT_LANGUAGE);
	    
	    if (dubCoreLang != null) {
	        // Assume this indicates the bug w/using CharsetDetector even when HTTP response lang exists,
	        // or we find language info in the meta-data.
	        if (httpEquivLang == dubCoreLang) {
	            if (httpRespLang != null) {
	                result = httpRespLang;
	            } else {
	                result = dubCoreLang;
	            }
	        } else {
	            result = dubCoreLang;
	        }
	    } else if (httpEquivLang != null) {
	        result = httpEquivLang;
	    } else {
	        result = httpRespLang;
	    }
	    
		
	    result = getFirstLanguage(result);
		
		if (result == null) {
			// Language is still unspecified, so use ProfileHandler's result
			LanguageIdentifier langIdentifier = profilingHandler.getLanguage();
			// FUTURE KKr - provide config for specifying required certainty level.
			if (langIdentifier.isReasonablyCertain()) {
			    result = langIdentifier.getLanguage();
				LOGGER.trace("Using language specified by profiling handler: " + result);
			} else {
			    result = "";
			}

		}
		
		return result;
	}

	private String getFirstLanguage(String lang) {
		if (lang != null && lang.length() > 0) {
			// TODO VMa -- DublinCore languages could be specified in a multiple of ways
			// see : http://dublincore.org/documents/2000/07/16/usageguide/qualified-html.shtml#language
			// This means that it is possible to get back 3 character language strings as per ISO639-2
			// For now, we handle just two character language strings and if we do get a 3 character string we 
			// treat it as a "null" language.
			
			// TODO VMa - what if the length is just one char ?
			if (lang.length() > 2) {
				Matcher m = LANGUAGE_CODE_PATTERN.matcher(lang);
				
				if (m.matches()) {
					lang = m.group(1);
				} else {
					lang = null;
				}
			}
		} 
		return lang;
	}
}
