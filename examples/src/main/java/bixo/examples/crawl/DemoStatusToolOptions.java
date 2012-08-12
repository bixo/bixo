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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class DemoStatusToolOptions {
    private boolean _debugLogging = false;

    private String _crawlDir;
    
    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        this._debugLogging = debugLogging;
    }

    @Option(name = "-crawldir", usage = "output directory of preceeding crawl", required = true)
    public void setCrawlDir(String crawlDir) {
    	_crawlDir = crawlDir;
    }

    @Option(name = "-exportdb", usage = "only export the crawldb", required = false)
    private boolean _exportDb = false;

    public String getCrawlDir() {
        return _crawlDir;
    }

    public boolean isDebugLogging() {
        return _debugLogging;
    }
    
    public boolean isExportDb() {
        return _exportDb;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
