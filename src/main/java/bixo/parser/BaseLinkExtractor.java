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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import bixo.datum.Outlink;

@SuppressWarnings("serial")
public abstract class BaseLinkExtractor extends DefaultHandler implements Serializable {

    public static final Set<String> DEFAULT_LINK_TAGS =
        new HashSet<String>() {{
            add("a");
        }};
    
    public static final Set<String> ALL_LINK_TAGS =
        new HashSet<String>() {{
            add("a");
            add("img");
            add("frame");
            add("iframe");
            add("link");
            add("area");
            add("input");
            add("bgsound");
            add("object");
            add("blockquote");
            add("q");
            add("ins");
            add("del");
            add("embed");
        }};
        
        public static final Set<String> DEFAULT_LINK_ATTRIBUTE_TYPES =
            new HashSet<String>() {{
                add("href");
            }};
            
    public static final Set<String> ALL_LINK_ATTRIBUTE_TYPES =
        new HashSet<String>() {{
            add("href");
            add("src");
            add("data");
            add("cite");
        }};

    protected String _inAnchorTag;        
    protected String _curUrl;
    protected String _curRelAttributes;
    protected StringBuilder _curAnchor = new StringBuilder();
    protected Set<String> _linkTags = DEFAULT_LINK_TAGS;
    protected Set<String> _linkAttributeTypes = DEFAULT_LINK_ATTRIBUTE_TYPES;

    /**
     * @param linkTags to collect {@link Outlink}s from
     * (defaults to {@link BaseLinkExtractor#DEFAULT_LINK_TAGS})
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * it processes. Instead, provide this set of link tags to
     * {@link bixo.config.ParserPolicy}.
     */
    public void setLinkTags(Set<String> linkTags) {
        _linkTags = linkTags;
    }

    public Set<String> getLinkTags() {
        return _linkTags;
    }
    
    /**
     * @param linkAttributeTypes to collect {@link Outlink}s from
     * (defaults to {@link BaseLinkExtractor#DEFAULT_LINK_ATTRIBUTE_TYPES})
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link attributes
     * it processes. Instead, provide this set of attributes to
     * {@link bixo.config.ParserPolicy}.
     */
    public void setLinkAttributeTypes(Set<String> linkAttributeTypes) {
        _linkAttributeTypes = linkAttributeTypes;
    }

    public Set<String> getLinkAttributeTypes() {
        return _linkAttributeTypes;
    }
    
    public void reset() {
        _inAnchorTag = null;
    }
    
    public void addLink(Outlink link) {};
    
    public abstract Outlink[] getLinks();
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        String tag = localName.toLowerCase();
        
        if ((_inAnchorTag == null) && _linkTags.contains(tag)) {
            for (String linkAttributeType : _linkAttributeTypes) {
                String attrValue = attributes.getValue(linkAttributeType);
                if (attrValue != null) {
                    _curUrl = attrValue;
                    _curRelAttributes = attributes.getValue("rel");
                    _inAnchorTag = tag;
                    _curAnchor.setLength(0);
                }
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        
        if (_inAnchorTag != null) {
            _curAnchor.append(ch, start, length);
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);

        if (localName.equalsIgnoreCase(_inAnchorTag)) {
            addLink(new Outlink(_curUrl, _curAnchor.toString(), _curRelAttributes));
            _inAnchorTag = null;
        }
    }

}
