package bixo.tools;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class SimpleStatusToolOptions {
    private boolean _debugLogging = false;

    private String _outputDir;
    
    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        this._debugLogging = debugLogging;
    }

    @Option(name = "-outputdir", usage = "output directory of preceeding crawl", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    public String getOutputDir() {
        return _outputDir;
    }

    public boolean isDebugLogging() {
        return _debugLogging;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
