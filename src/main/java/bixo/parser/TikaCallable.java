/**
 * 
 */
package bixo.parser;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.ProfilingHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.TeeContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import bixo.datum.BaseDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;

class TikaCallable implements Callable<ParsedDatum> {
    private static final Logger LOGGER = Logger.getLogger(TikaCallable.class);
    
    // Simplistic language code pattern used when there are more than one languages specified
    // FUTURE KKr - improve this to handle en-US, and "eng" for those using old-style language codes.
    private static final Pattern LANGUAGE_CODE_PATTERN = Pattern.compile("([a-z]{2})([,;-]).*");

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


    private Parser _parser;
    private InputStream _input;
    private Metadata _metadata;
    
    public TikaCallable(Parser parser, InputStream input, Metadata metadata) {
        _parser = parser;
        _input = input;
        _metadata = metadata;
    }
    
    @Override
    public ParsedDatum call() throws Exception {
        try {
            LinkBodyHandler handler = new LinkBodyHandler();
            ProfilingHandler profilingHandler = new ProfilingHandler();  
            TeeContentHandler teeContentHandler = new TeeContentHandler(handler, profilingHandler);

            _parser.parse(_input, teeContentHandler, _metadata, new ParseContext());
            
            String lang = detectLanguage(_metadata, profilingHandler);
            return new ParsedDatum(_metadata.get(Metadata.RESOURCE_NAME_KEY), handler.getContent(), lang,
                            _metadata.get(Metadata.TITLE),
                            handler.getLinks(), makeMap(_metadata), BaseDatum.EMPTY_METADATA_MAP);
        } catch (Exception e) {
            // Generic exception that's OK to re-throw
            throw e;
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("Attempting to use excluded parser");
        } catch (Throwable t) {
            // Make sure nothing inside Tika can kill us
            throw new RuntimeException("Serious shut-down error thrown from Tika", t);
        }
    }
    
    /**
     * See if a language was set by the parser, from meta tags.
     * As a last resort falls back to the result from the ProfilingHandler.
     *  
     * @param metadata
     * @param profilingHandler
     * @return The first language found (two char lang code) or empty string if no language was detected.
     */
    private static String detectLanguage(Metadata metadata, ProfilingHandler profilingHandler) {
        String result = null;
        
        String dubCoreLang = metadata.get(Metadata.LANGUAGE);
        String httpEquivLang = metadata.get(Metadata.CONTENT_LANGUAGE);
        
        if (dubCoreLang != null) {
            result = dubCoreLang;
        } else if (httpEquivLang != null) {
            result = httpEquivLang;
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

    private static Map<String, String> makeMap(Metadata metadata) {
        Map<String, String> result = new HashMap<String, String>();
        
        for (String key : metadata.names()) {
            result.put(key, metadata.get(key));
        }
        
        return result;
    }


    private static String getFirstLanguage(String lang) {
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