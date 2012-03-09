package com.scaleunlimited.helpful.tools;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class AnalyzeMboxOptions {
    private boolean _debugLogging = false;

    private String _inputFile;
    private String _outputDir;
    

    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        this._debugLogging = debugLogging;
    }

    @Option(name = "-inputfile", usage = "input file with URLs, or a .mbox file", required = true)
    public void setInputFile(String inputFile) {
        _inputFile = inputFile;
    }

    @Option(name = "-outputdir", usage = "output directory", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    public String getInputFile() {
        return _inputFile;
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
