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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class DemoCrawlAndMinerToolOptions extends BaseCrawlToolOptions {

    private boolean _cleanOutputDir = false;
    private boolean _generateHTML = false;
    private boolean _enableMiner = false;
    private String _regexUrlToMineFile = null;//default to mine all fetched pages

    @Option(name = "-urlstomine", usage = "text file containing list of regex patterns for urls to mine", required = false)
    public void setRegexUrlToMineFile(String regexFiltersFile) {
        _regexUrlToMineFile = regexFiltersFile;
    }

    public String getRegexUrlToMineFile() {
        return _regexUrlToMineFile ;

    }

    @Option(name = "-clean", usage = "Delete the output dir if it exists - WARNING:you won't be prompted!", required = false)
    public void setCleanOutputDir(boolean cleanOutputDir) {
        _cleanOutputDir = cleanOutputDir;
    }

    public boolean isCleanOutputDir() {
        return _cleanOutputDir  ;
    }

    @Option(name = "-html", usage = "Generate HTML output as a text file", required = false)
    public void setGenerateHTML(boolean generateHTML) {
        _generateHTML = generateHTML;
    }

    public boolean isGenerateHTML() {
        return _generateHTML  ;
    }

    @Option(name = "-enableminer", usage = "Generate miner output as a text file", required = false)
    public void setEnableMiner(boolean generateHTML) {
        _enableMiner = generateHTML;
    }

    public boolean isEnableMiner() {
        return _enableMiner  ;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
