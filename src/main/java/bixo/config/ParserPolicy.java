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
package bixo.config;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.Set;

import bixo.parser.BaseLinkExtractor;

/**
 * Definition of policy for parsing.
 * 
 */
@SuppressWarnings("serial")
public class ParserPolicy implements Serializable {
    
    public static final int NO_MAX_PARSE_DURATION = Integer.MAX_VALUE;
    
    public static final int DEFAULT_MAX_PARSE_DURATION = 30 * 1000;
    
    private int _maxParseDuration;        // Max # of milliseconds to wait for parse to complete a document.
    
    private Set<String> _linkTags;
    
    private Set<String> _linkAttributeTypes;
    
    public ParserPolicy() {
        this(DEFAULT_MAX_PARSE_DURATION);
    }

    public ParserPolicy(int maxParseDuration) {
        this(   maxParseDuration,
                BaseLinkExtractor.DEFAULT_LINK_TAGS,
                BaseLinkExtractor.DEFAULT_LINK_ATTRIBUTE_TYPES);
    }

    public ParserPolicy(int maxParseDuration,
                        Set<String> linkTags,
                        Set<String> linkAttributeTypes) {
        if ((maxParseDuration <= 0) && (maxParseDuration != NO_MAX_PARSE_DURATION)) {
            throw new InvalidParameterException("maxParseDuration must be > 0: " + maxParseDuration);
        }
        
        // Catch common error of specifying maxParseDuration in seconds versus milliseconds
        if (maxParseDuration < 100)  {
            throw new InvalidParameterException("maxParseDuration must be milliseconds, not seconds: " + maxParseDuration);
        }
        
        _maxParseDuration = maxParseDuration;
        _linkAttributeTypes = linkAttributeTypes;
        _linkTags = linkTags;
    }

    public int getMaxParseDuration() {
        return _maxParseDuration;
    }
       
    public void setMaxParseDuration(int maxParseDuration) {
        _maxParseDuration = maxParseDuration;
    }

    public Set<String> getLinkTags() {
        return _linkTags;
    }

    public void setLinkTags(Set<String> linkTags) {
        _linkTags = linkTags;
    }

    public Set<String> getLinkAttributeTypes() {
        return _linkAttributeTypes;
    }

    public void setLinkAttributeTypes(Set<String> linkAttributeTypes) {
        _linkAttributeTypes = linkAttributeTypes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_linkAttributeTypes == null) ? 0 : _linkAttributeTypes.hashCode());
        result = prime * result + ((_linkTags == null) ? 0 : _linkTags.hashCode());
        result = prime * result + _maxParseDuration;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ParserPolicy other = (ParserPolicy) obj;
        if (_linkAttributeTypes == null) {
            if (other._linkAttributeTypes != null)
                return false;
        } else if (!_linkAttributeTypes.equals(other._linkAttributeTypes))
            return false;
        if (_linkTags == null) {
            if (other._linkTags != null)
                return false;
        } else if (!_linkTags.equals(other._linkTags))
            return false;
        if (_maxParseDuration != other._maxParseDuration)
            return false;
        return true;
    }

    @Override
	public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("Max parse duration: " + getMaxParseDuration());
        result.append('\r');
        result.append("Link tags: " + getLinkTags());
        result.append('\r');
        result.append("Link attribute types: " + getLinkAttributeTypes());
        
        return result.toString();
    }
}
