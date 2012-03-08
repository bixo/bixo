/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
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
