/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.parser;

import java.io.StringWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/*
 * HtmlcontentExtractor is a content extractor that returns as content the 
 * raw (cleaned) HTML, with all of the tags.
 */
@SuppressWarnings("serial")
public class HtmlContentExtractor extends BaseContentExtractor {
    
    private  ContentHandler _contentHandler = null;
    private transient StringWriter _stringWriter = null;
    private String _method;
    
    public HtmlContentExtractor() {
        this("html");
    }
    
    public HtmlContentExtractor(String method) {
        _method = method;
    }
    
    
    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        _contentHandler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        _contentHandler.setDocumentLocator(locator);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        _contentHandler.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        _contentHandler.endPrefixMapping(prefix);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        _contentHandler.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        _contentHandler.skippedEntity(name);
    }

    @Override
    public void startDocument() throws SAXException {
        try {
            init();
        } catch (TransformerConfigurationException e) {
            throw new SAXException("Error initializing transform handler: " + e.getMessage());
        }
        _contentHandler.startDocument();
    }
    
    @Override
    public void endDocument() throws SAXException {
        _contentHandler.endDocument();
        _stringWriter.flush();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        _contentHandler.startElement(uri, localName, qName, atts);
    }    
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        _contentHandler.characters(ch, start, length);
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        _contentHandler.endElement(uri, localName, qName);
    }
    
    @Override
    public String getContent() {
        return _stringWriter.toString();
    }

    @Override
    public void reset() {
        if (_stringWriter != null) {
            _stringWriter.flush();
            _stringWriter.getBuffer().setLength(0);
        }
    }
    /**
     * Returns a transformer handler that serializes incoming SAX events
     * to XHTML or HTML (depending the given method) using the given output
     * encoding.
     *
     * @param method "xml" or "html"
     * @param encoding output encoding,
     *                 or <code>null</code> for the platform default
     * @return {@link System#out} transformer handler
     * @throws TransformerConfigurationException
     *         if the transformer can not be created
     */
    private static TransformerHandler getTransformerHandler(
            String method, String encoding)
            throws TransformerConfigurationException {
        SAXTransformerFactory factory = (SAXTransformerFactory)
                SAXTransformerFactory.newInstance();
        TransformerHandler handler = factory.newTransformerHandler();
        handler.getTransformer().setOutputProperty(OutputKeys.METHOD, method);
        handler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
        if (encoding != null) {
            handler.getTransformer().setOutputProperty(
                    OutputKeys.ENCODING, encoding);
        }
        return handler;
    }

    protected synchronized void init() throws TransformerConfigurationException {
        if (_contentHandler == null) {
            _stringWriter = new StringWriter();
            TransformerHandler handler = getTransformerHandler(_method, "UTF-8");
            handler.setResult(new StreamResult(_stringWriter));
            _contentHandler = handler;

        }
    }
}
