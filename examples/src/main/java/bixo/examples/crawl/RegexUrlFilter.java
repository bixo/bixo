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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlFilter;

// Filter URLs that fall outside of the target domain
@SuppressWarnings("serial")
public class RegexUrlFilter extends BaseUrlFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegexUrlFilter.class);

    private static final String INCLUDE_CHAR = "+";
    private static final String EXCLUDE_CHAR = "-";

    private static final String COMMENT_CHAR = "#";

    private ArrayList<SimpleImmutableEntry<Pattern, Boolean>> _domainExclusionInclusionList = new ArrayList<SimpleImmutableEntry<Pattern, Boolean>>();


    /**
     * Sets up the regex based url filter based on the list provided by the caller.
     * Note : The filters are applied in the order defined in the list.
     * @param exclusionInclusionPatterns
     */
    public RegexUrlFilter(String[] exclusionInclusionPatterns) {
        initExclusionInclusionList(exclusionInclusionPatterns);
    }
    
    private void initExclusionInclusionList(String[] exclusionInclusionPatterns) {
        
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


    @Override
    public boolean isRemove(UrlDatum datum) {
        String urlAsString = datum.getUrl();
        
        
        if (!_domainExclusionInclusionList.isEmpty()) {
            for (Entry<Pattern, Boolean> e : _domainExclusionInclusionList) {
                if (e.getKey().matcher(urlAsString).find()) {
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
    
    public static List<String> getDefaultUrlFilterPatterns() throws IOException {
        InputStream is = RegexUrlFilter.class.getResourceAsStream("/regex-url-filters.txt");
        DataInputStream in = new DataInputStream(is);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        return readFilters(reader);
    }

    public static List<String> getUrlFilterPatterns(String urlFiltersFile) throws IOException  {
        FileReader fileReader = new FileReader(urlFiltersFile);
        BufferedReader reader = new BufferedReader(fileReader);
        return readFilters(reader);
        
    }

    private static List<String> readFilters(BufferedReader reader) throws IOException {
        List<String> filterList = new ArrayList<String>();
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (StringUtils.isNotBlank(line) && !line.startsWith(COMMENT_CHAR)) {
                filterList.add(line.trim());
            }
        }
        reader.close();
        return filterList;
    }
}
