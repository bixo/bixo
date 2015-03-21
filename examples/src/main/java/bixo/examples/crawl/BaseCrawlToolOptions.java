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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

import bixo.config.BixoPlatform.Platform;

public class BaseCrawlToolOptions {

    public static final int NO_CRAWL_DURATION = 0;
    public static final int DEFAULT_MAX_THREADS = 10;
    private static final int DEFAULT_NUM_LOOPS = 1;
    private static final String DEFAULT_LOGS_DIR = "logs";
    
    private String _loggingAppender = null;
    private boolean _debugLogging = false;

    private String _outputDir;
    private String _agentName;
    private String _domain;
    private String _urlsFile;
    private int _crawlDuration = NO_CRAWL_DURATION;
    private int _maxThreads = DEFAULT_MAX_THREADS;
    private int _numLoops = DEFAULT_NUM_LOOPS;
    private boolean _useBoilerpipe = false;
    private String _regexUrlFiltersFile = null;;
    private String _logsDir = DEFAULT_LOGS_DIR;
    private boolean _localPlatformMode;

    
    @Option(name = "-domain", usage = "domain to crawl (e.g. cnn.com)", required = false)
    public void setDomain(String domain) {
        _domain = domain;
    }

    @Option(name = "-urls", usage = "text file containing list of urls (either -domain or -urls needs to be set)", required = false)
    public void setUrlsFile(String urlsFile) {
        _urlsFile = urlsFile;
    }

    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        _debugLogging = debugLogging;
    }

    @Option(name = "-logger", usage = "set logging appender (console, DRFA)", required = false)
    public void setLoggingAppender(String loggingAppender) {
        _loggingAppender = loggingAppender;
    }
    
    @Option(name = "-outputdir", usage = "output directory", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    @Option(name = "-agentname", usage = "user agent name", required = true)
    public void setAgentName(String agentName) {
        _agentName = agentName;
    }

    @Option(name = "-maxthreads", usage = "maximum number of fetcher threads to use", required = false)
    public void setMaxThreads(int maxThreads) {
        _maxThreads = maxThreads;
    }

    @Option(name = "-numloops", usage = "number of fetch/update loops", required = false)
    public void setNumLoops(int numLoops) {
        _numLoops = numLoops;
    }

    @Option(name = "-duration", usage = "target crawl duration in minutes", required = false)
    public void setCrawlDuration(int crawlDuration) {
        _crawlDuration = crawlDuration;
    }
    
    @Option(name = "-boilerpipe", usage = "Use Boilerpipe library when parsing", required = false)
    public void setUseBoilerpipe(boolean useBoilerpipe) {
        _useBoilerpipe = useBoilerpipe;
    }

    @Option(name = "-urlfilters", usage = "text file containing list of regex patterns for url filtering", required = false)
    public void setRegexUrlFiltersFile(String regexFiltersFile) {
        _regexUrlFiltersFile = regexFiltersFile;
    }

    @Option(name = "-logsdir", usage = "local fs dir to store loop specific logs [optional: default=logs]", required = false)
    public void setLogsDir(String logsDir) {
        _logsDir = logsDir;
    }

    @Option(name = "-localplatform", usage = "Use BixoPlatform in Local mode [optional: default=false]", required = false)
    public void setLocalPlatformMode(boolean mode) {
        _localPlatformMode = mode;
    }

    public String getOutputDir() {
        return _outputDir;
    }

    public String getDomain() {
        return _domain;
    }

    public String getUrlsFile() {
        return _urlsFile;
    }

    public String getAgentName() {
        return _agentName;
    }
    
    public int getMaxThreads() {
        return _maxThreads;
    }

    public int getNumLoops() {
        return _numLoops;
    }

    public int getCrawlDuration() {
        return _crawlDuration;
    }

    public boolean isDebugLogging() {
        return _debugLogging;
    }
    
    public String getLoggingAppender() {
        return _loggingAppender;
    }

    public boolean isUseBoilerpipe() {
        return _useBoilerpipe ;
    }
    
    public String getRegexUrlFiltersFile() {
        return _regexUrlFiltersFile ;
        
    }

    public String getLogsDir() {
        return _logsDir ;
        
    }
    
    public boolean isLocalPlatformMode() {
        return _localPlatformMode;
    }

    public Platform getPlatformMode() {
        if (_localPlatformMode) {
            return Platform.Local;
        }
        return Platform.Hadoop;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }



}
