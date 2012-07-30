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

public class NotSoSimpleCrawlToolOptions {

    public static final int NO_CRAWL_DURATION = 0;
    public static final int DEFAULT_MAX_THREADS = 10;
    private static final int DEFAULT_NUM_LOOPS = 1;

    private String _loggingAppender = null;
    private boolean _debugLogging = false;
    private boolean _overwrite = false;
    private boolean _useBoilerpipe = false;

    private String _outputDir;
    private String _mahoutDir;
    private String _agentName;
    private String _domain;
    private String _urlsFile = "";//seeds for crawl
    private String _filterFile = "";//url pattern to filter out or in
    private String _languageName = "";//single string, if set then only allow a language that contains it, "en" or "es" for example
    
    private int _crawlDuration = NO_CRAWL_DURATION;
    private int _maxThreads = DEFAULT_MAX_THREADS;
    private int _numLoops = DEFAULT_NUM_LOOPS;

    
    @Option(name = "-domain", usage = "domain to crawl (e.g. cnn.com)", required = false)
    public void setDomain(String domain) {
        _domain = domain;
    }

    @Option(name = "-urls", usage = "text file containing list of urls (either -domain or -urls needs to be set)", required = false)
    public void setUrlsFile(String urlsFile) {
        _urlsFile = urlsFile;
    }

    @Option(name = "-filterfile", usage = "text file containing patterns used to filter in or out urls the crawl (if set all urls must match an include pattern in this file)", required = false)
    public void setBlacklistFile(String filterFile) {
        _filterFile = filterFile;
    }

    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        _debugLogging = debugLogging;
    }

    @Option(name = "-ow", usage = "overwrite previous crawl, (warning, this will delete the crawl in the outputdir)", required = false)
    public void setOverwrite(boolean overwrite) {
        _overwrite = overwrite;
    }

    @Option(name = "-useboilerpipe", usage = "use boilerpipe to parse HTML (good at removing boilerplate, menus, footers, etc.)", required = false)
    public void setUseBoilerpipe(boolean useBoilerpipe) {
        _useBoilerpipe = useBoilerpipe;
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

    @Option(name = "-parselanguage", usage = "identify and save to mouhoutdir only pages matching this language (for example 'en' or 'es')", required = false)
    public void setLanguageName(String languageName) {
        _languageName = languageName;
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

    @Option(name = "-mahoutdir", usage = "put all parsed text files here in mahout sequence file format", required = true)
    public void setMahoutDir(String mahoutDir) {
        _mahoutDir = mahoutDir;
    }

    public String getOutputDir() {
        return _outputDir;
    }

    public String getMahoutDir() {
        return _mahoutDir;
    }

   public String getDomain() {
        return _domain;
    }

    public String getUrlsFile() {
        return _urlsFile;
    }

    public String getLanguageName() {
        return _languageName;
    }

    public String getFilterFile() {
        return _filterFile;
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

    public boolean getOverwrite() {
        return _overwrite;
    }

    public boolean getUseBoilerpipe() {
        return _useBoilerpipe;
    }

    public String getLoggingAppender() {
        return _loggingAppender;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }



}
