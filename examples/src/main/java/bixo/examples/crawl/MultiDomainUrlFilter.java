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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.regex.Pattern;

// pferrel: filter creates a list of patterns processed in order. The file they are taken from contains
// one regex string per line. Any regex prefixed with a "+" which matches a url will allow it into the crawl
// If the regex is prefixed with "-" and matches a url it will be filtered out of the crawl.

@SuppressWarnings("serial")
public class MultiDomainUrlFilter extends BaseUrlFilter {
    private static final Logger LOGGER = Logger.getLogger(MultiDomainUrlFilter.class);

    private ArrayList<ArrayList<Object>> _filters = new ArrayList<ArrayList<Object>>();
    private Pattern _suffixExclusionPattern;
    private Pattern _protocolInclusionPattern;



    private MultiDomainUrlFilter(){
        _filters.clear();
        _suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe)$");
        _protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");
    }

    public MultiDomainUrlFilter( Path filterFile) throws Exception {
        //we could require a filter file and put these in all urls or leave them here
        _suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe)$");
        _protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");

        JobConf conf = HadoopUtils.getDefaultJobConf();

        try {//process the file passed in
            if( filterFile != null){
                FileSystem fs = filterFile.getFileSystem(conf);
                if(fs.exists(filterFile)){
                    FSDataInputStream in = fs.open(filterFile);
                    LineReader lr = new LineReader(in);
                    Text tmpStr = new Text();
                    while( lr.readLine(tmpStr) > 0 && !tmpStr.toString().equals("")){//skip blank lines
                        String p = tmpStr.toString().trim();//remove whitespace
                        if( p.substring(0,1).equals("+")){// '+' means do-crawl
                            ArrayList filterPair = new ArrayList();
                            filterPair.add((Boolean)true);
                            filterPair.add(Pattern.compile(p.substring(1,p.length())));
                            _filters.add(filterPair);
                        } else if(p.substring(0,1).equals("-")) {// '-' means filter out
                            ArrayList filterPair = new ArrayList();
                            filterPair.add(new Boolean(false));
                            filterPair.add(Pattern.compile(p.substring(1,p.length())));
                            _filters.add(filterPair);
                        }// otherwise a comment or malformed filter pattern
                    }
                }
            }

        } catch (Exception e) {
            //any cleanup here? This would indicate a file system error, most likely
            throw e;
        }
    }

    @Override
    public boolean isRemove(UrlDatum datum) {
        String urlAsString = datum.getUrl();

        // Skip URLs with protocols we don't want to try to process
        if (!_protocolInclusionPattern.matcher(urlAsString).find()) {
            return true;
        }

        if (_suffixExclusionPattern.matcher(urlAsString).find()) {
            return true;
        }

        if( !_filters.isEmpty() ){
            for( ArrayList d : _filters ){
                if( ((Pattern)(d.get(1))).matcher(urlAsString).find()){
                    if((Boolean)d.get(0)){
                        return false;//do not remove since this is an include pattern
                    } else {
                        return true;//remove since this is a filter-out pattern
                    }
                }
            }
            return true;// if it doesn't match any pattern, then by default remove
        }
        return false;//no filters so do not remove the url
    }
}
