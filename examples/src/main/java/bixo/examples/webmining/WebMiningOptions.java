/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class WebMiningOptions {
    
    private String _username;
    private String _workingDir = "build/test/working";
    private String _pageAnalyzer = "com.scaleunlimited.webmining.SimplePageAnalyzer";
    private String _solrUrl = null;
    
    @Option(name = "-username", usage = "unique username (no spaces, all ascii)", required = true)
    public void setUsername(String username) {
        _username = username;
    }

    public String getUsername() {
        return _username;
    }

    @Option(name = "-workingdir", usage = "path to directory for fetching", required = false)
    public void setWorkingDir(String workingDir) {
        _workingDir = workingDir;
    }

    @Option(name = "-analyzer", usage = "fully specified classname for analyzer", required = false)
    public void setAnalyzer(String analyzer) {
        _pageAnalyzer = analyzer;
    }

    @Option(name = "-solrurl", usage = "url to solr server", required = false)
    public void setSolrUrl(String solrUrl) {
        _solrUrl  = solrUrl;
    }

    public String getWorkingDir() {
        return _workingDir;
    }

    public String getAnalyzer() {
        return _pageAnalyzer;
    }

    public String getSolrUrl() {
        return _solrUrl;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }


}
