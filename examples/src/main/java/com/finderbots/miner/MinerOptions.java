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
package com.finderbots.miner;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class MinerOptions {

    private static final int DEFAULT_NUM_LOOPS = 1;
    private static final String DEFAULT_WORKING_DIR = "crawl";

    private String _workingDir = DEFAULT_WORKING_DIR;
    private String _agentName;
    private int _numLoops = DEFAULT_NUM_LOOPS;
    private String _urlsFile;//have to supply a URL seed file
    private String _regexUrlFiltersFile = null;//default to not filter urls for fetching
    private String _regexUrlToMineFile = null;//default to mine all fetched pages
    private Boolean _overwriteCrawl = false;//default to reprocess already fetched data



    @Option(name = "-overwritecrawl", usage = "Delete the output dir and start mining from scratch: otherwise will try to reprocess previously fetched data if it exists", required = false)
    public void setCleanOutputDir(boolean overwriteCrawl) {
        _overwriteCrawl = overwriteCrawl;
    }

    public boolean isCleanOutputDir() {
        return _overwriteCrawl  ;
    }

    @Option(name = "-workingdir", usage = "path to directory for fetching", required = false)
    public void setWorkingDir(String workingDir) {
        _workingDir = workingDir;
    }

    public String getWorkingDir() {
        return _workingDir;
    }

    @Option(name = "-agentname", usage = "user agent name", required = true)
    public void setAgentName(String agentName) {
        _agentName = agentName;
    }

    public String getAgentName() {
        return _agentName;
    }

    @Option(name = "-numloops", usage = "number of fetch/update loops", required = false)
    public void setNumLoops(int numLoops) {
        _numLoops = numLoops;
    }

    public int getNumLoops() {
        return _numLoops;
    }

    @Option(name = "-urlfilters", usage = "text file containing list of regex patterns for url filtering", required = false)
    public void setRegexUrlFiltersFile(String regexFiltersFile) {
        _regexUrlFiltersFile = regexFiltersFile;
    }

    public String getRegexUrlFiltersFile() {
        return _regexUrlFiltersFile ;

    }

    @Option(name = "-urlstomine", usage = "text file containing list of regex patterns for urls to mine", required = false)
    public void setRegexUrlToMineFile(String regexFiltersFile) {
        _regexUrlToMineFile = regexFiltersFile;
    }

    public String getRegexUrlToMineFile() {
        return _regexUrlToMineFile ;

    }

    @Option(name = "-urls", usage = "text file containing list of urls to seed the crawl", required = true)
    public void setUrlsFile(String urlsFile) {
        _urlsFile = urlsFile;
    }

    public String getUrlsFile() {
        return _urlsFile;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
