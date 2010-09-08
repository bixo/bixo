package bixo.tools;

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
