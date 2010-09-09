/*
 * Copyright (c) 2010 TransPac Software, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.examples;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class SimpleCrawlToolOptions {

    public static final int NO_CRAWL_DURATION = 0;
    public static final int DEFAULT_MAX_THREADS = 10;
    private static final int DEFAULT_NUM_LOOPS = 1;

    private String _loggingAppender = null;
    private boolean _debugLogging = false;

    private String _outputDir;
    private String _agentName;
    private String _domain;
    
    private int _crawlDuration = NO_CRAWL_DURATION;
    private int _maxThreads = DEFAULT_MAX_THREADS;
    private int _numLoops = DEFAULT_NUM_LOOPS;

    
    @Option(name = "-domain", usage = "domain to crawl (e.g. cnn.com)", required = true)
    public void setDomain(String domain) {
        _domain = domain;
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

    public String getOutputDir() {
        return _outputDir;
    }

    public String getDomain() {
        return _domain;
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

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }



}
