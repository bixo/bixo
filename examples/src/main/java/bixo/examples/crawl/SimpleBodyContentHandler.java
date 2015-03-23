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
package bixo.examples.crawl;

import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class SimpleBodyContentHandler extends ContentHandlerDecorator {

    private boolean _inBody;
    
    public SimpleBodyContentHandler() {
        this(new WriteOutContentHandler());
    }
    
    public SimpleBodyContentHandler(ContentHandler handler) {
        super(handler);
    }
    
    @Override
    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        if (localName.equalsIgnoreCase("body")) {
            _inBody = true;
        }
        
        super.startElement(uri, localName, name, atts);
    }
    
    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        if (localName.equalsIgnoreCase("body")) {
            _inBody = false;
        }
        
        super.endElement(uri, localName, name);
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (_inBody) {
            super.characters(ch, start, length);
        }
    }
    
    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (_inBody) {
            super.ignorableWhitespace(ch, start, length);
        }
    }
}
