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

import java.util.ArrayList;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import bixo.datum.Outlink;

@SuppressWarnings("serial")
public class SimpleLinkExtractor extends BaseLinkExtractor {
    private boolean _inHead;
    private boolean _skipLinks;
    private List<Outlink> _outlinks = new ArrayList<Outlink>();

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws org.xml.sax.SAXException {
        super.startElement(uri, localName, qName, attributes);

        if (!_inHead && localName.equalsIgnoreCase("head")) {
            _inHead = true;
        } else if (_inHead && localName.equalsIgnoreCase("meta")) {
            // See if we have a robots directive
            String attrName = attributes.getValue("name");
            String content = attributes.getValue("content");
            if ((attrName != null) && attrName.equalsIgnoreCase("robots") && (content != null)) {
                String[] directives = content.split(",");
                for (String directive : directives) {
                    directive = directive.trim().toLowerCase();
                    if (directive.equals("none") || directive.equals("nofollow")) {
                        _skipLinks = true;
                        break;
                    }
                }
            }
        }
    };
    
    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);

        if (_inHead && localName.equalsIgnoreCase("head")) {
            _inHead = false;
        }
    }

    @Override
    public void reset() {
        super.reset();
        _outlinks.clear();
        _inHead = false;
        _skipLinks = false;
    }
    

    @Override
    public void addLink(Outlink link) {
        if (!_skipLinks) {
            _outlinks.add(link);
        }
    }

    @Override
    public Outlink[] getLinks() {
        return _outlinks.toArray(new Outlink[_outlinks.size()]);
    }
}