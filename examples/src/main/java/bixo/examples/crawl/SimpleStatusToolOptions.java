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
package bixo.examples.crawl;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class SimpleStatusToolOptions {
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
