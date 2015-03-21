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
package bixo.examples.webmining;

import bixo.datum.Outlink;

public abstract class BasePageAnalyzer {

    private String _hostName;
    private String _url;
    private String _htmlContent;
    private Outlink[] _outlinks;
    
    /**
     * Called for each new page, before any of the getXXX methods.
     * 
     * @param hostname - page's domain, including sub-domains.
     * @param url - URL to page
     * @param htmlContent - cleaned up HTML
     * @param outlinks - links extracted from text
     */
    public void reset(String hostname, String url, String htmlContent, Outlink[] outlinks) throws Exception {
        _hostName = hostname;
        _url = url;
        _htmlContent = htmlContent;
        _outlinks = outlinks;
    }
    
    public String getHostName() {
        return _hostName;
    }

    public String getUrl() {
        return _url;
    }

    public String getHtmlContent() {
        return _htmlContent;
    }

    public Outlink[] getLinks() throws Exception {
        return _outlinks;
    }

    public abstract PageResult[] getResults() throws Exception;
    
    public abstract float getScore() throws Exception;
}
