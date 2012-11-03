/*
 * Copyright 2009-2012 Scale Unlimited
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
package com.finderbots.miner2;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.regex.Pattern;

// Filter URLs that fall outside of the target domain
@SuppressWarnings("serial")
public class RegexUrlStringFilter implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(RegexUrlStringFilter.class);

    private static final String INCLUDE_CHAR = "+";
    private static final String EXCLUDE_CHAR = "-";

    private static final String COMMENT_CHAR = "#";

    private ArrayList<SimpleImmutableEntry<Pattern, Boolean>> _domainExclusionInclusionList = new ArrayList<SimpleImmutableEntry<Pattern, Boolean>>();

    /**
     * Sets up the regex based url filter based on the list provided by the caller.
     * Note : The filters are applied in the order defined in the list.
     * @param exclusionInclusionPatterns
     */
    public RegexUrlStringFilter(String[] exclusionInclusionPatterns) {
        initExclusionInclusionList(exclusionInclusionPatterns);
    }

    protected void initExclusionInclusionList(String[] exclusionInclusionPatterns) {
        
         if (exclusionInclusionPatterns != null) {
            for (String line : exclusionInclusionPatterns) {
                String p = line.trim();
                if (p.length() > 0) {
                    if (p.startsWith(INCLUDE_CHAR)) {
                        SimpleImmutableEntry<Pattern, Boolean> e = new SimpleImmutableEntry<Pattern, Boolean>(Pattern.compile(p.substring(1, p.length())), true);
                        _domainExclusionInclusionList.add(e);
                    } else if (p.startsWith(EXCLUDE_CHAR)) {
                        SimpleImmutableEntry<Pattern, Boolean> e = new SimpleImmutableEntry<Pattern, Boolean>(Pattern.compile(p.substring(1, p.length())), false);
                        _domainExclusionInclusionList.add(e);
                    } else {
                        LOGGER.warn(String.format("Invalid pattern - pattern should begin with either %s or %s", INCLUDE_CHAR, EXCLUDE_CHAR));
                    }
                }
            }
        } 
    }


    public boolean isRemove(String url) {

        if (!_domainExclusionInclusionList.isEmpty()) {
            for (Entry<Pattern, Boolean> e : _domainExclusionInclusionList) {
                if (e.getKey().matcher(url).find()) {
                    if (e.getValue() == true) {
                        return false;   // It's an include pattern - so do not remove
                    } else {
                        return true;    // Remove this since it is a filter-out pattern
                    }
                }
            }
            return true;    // if it doesn't match any pattern, then by default remove
        }
        
        return false;       // No filters so do not remove the url
    }

}
