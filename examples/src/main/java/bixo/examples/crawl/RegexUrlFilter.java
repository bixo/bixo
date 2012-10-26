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
package bixo.examples.crawl;

import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlFilter;
import com.bixolabs.cascading.HadoopUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

// Filter URLs that fall outside of the target domain
@SuppressWarnings("serial")
public class RegexUrlFilter extends BaseUrlFilter {
    private static final Logger LOGGER = Logger.getLogger(RegexUrlFilter.class);

    private static final String INCLUDE_CHAR = "+";
    private static final String EXCLUDE_CHAR = "-";

    private static final String COMMENT_CHAR = "#";
    private static final String DEFAULT_FILTER_LIST = "-(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe)$";

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
//        InputStream is = RegexUrlFilter.class.getResourceAsStream("/regex-url-filters.txt");
//        DataInputStream in = new DataInputStream(is);
//        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
//        return readFilters(reader);
//  pferrel: the above is hard to make work with both BufferedReader and LineReader since one works with Text and one with String
//  pferrel: so I changed the default regex to be stored in a static string instead of a file in the resources dir.
//  pferrel: I suggest the "regex-url-filters.txt" file be put in the same place the DemoCrawlTool is run from as an example
//  pferrel: filter file to build from. The logic here is that a user of the command line is not as likely to look in the resource
//  pferrel: directory to find examples as a programmer modifying the code.
        List<String> defaultFilters = new ArrayList<String>();
        defaultFilters.add(DEFAULT_FILTER_LIST);
        return defaultFilters;
    }

    public static List<String> getUrlFilterPatterns(String urlFiltersFile) throws IOException, InterruptedException {
        //this reads regex filters from a file in HDFS or the native file sysytem
        JobConf conf = HadoopUtils.getDefaultJobConf();
        Path filterFile = new Path(urlFiltersFile);
        FileSystem fs = filterFile.getFileSystem(conf);
        List<String> filterList = new ArrayList<String>();
        if(fs.exists(filterFile)){
            FSDataInputStream in = fs.open(filterFile);
            LineReader reader = new LineReader(in);
            Text tLine = new Text();
            while (reader.readLine(tLine) > 0 ) {
                String line = tLine.toString();
                if (StringUtils.isNotBlank(line) && (line.startsWith(INCLUDE_CHAR) || line.startsWith(EXCLUDE_CHAR))) {
                    filterList.add(line.trim());
                }
            }
            in.close();
        }
        return filterList;
    }
}
