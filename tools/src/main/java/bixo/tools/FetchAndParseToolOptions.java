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
package bixo.tools;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

import bixo.config.FetcherPolicy;

public class FetchAndParseToolOptions {
    private int _maxRedirects = 100;
    private int _maxRetries = 100;
    private int _maxSize = FetcherPolicy.DEFAULT_MAX_CONTENT_SIZE;
    private boolean _traceLogging = false;
    private String _urls;
    
    @Option(name = "-maxredirects", usage = "Maximum number of redirects", required = false)
    public void setMaxRedirects(int maxRedirects) {
        _maxRedirects = maxRedirects;
    }

    public int getMaxRedirects() {
        return _maxRedirects;
    }
    
    @Option(name = "-maxretries", usage = "Maximum number of retries", required = false)
    public void setMaxRetries(int maxRetries) {
        _maxRetries = maxRetries;
    }

    public int getMaxRetries() {
        return _maxRetries;
    }
    
    @Option(name = "-maxsize", usage = "Maximum bytes of content read", required = false)
    public void setMaxSize(int maxSize) {
        _maxSize = maxSize;
    }

    public int getMaxSize() {
        return _maxSize;
    }
    
    @Option(name = "-urls", usage = "Comma separated list of URLs to fetch", required = false)
    public void setUrls(String urls) {
        _urls = urls;
    }

    public String getUrls() {
        return _urls;
    }
    
    @Option(name = "-trace", usage = "Set logging level to trace", required = false)
    public void setTraceLogging(boolean traceLogging) {
        _traceLogging = traceLogging;
    }

    public boolean isTraceLogging() {
        return _traceLogging;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
