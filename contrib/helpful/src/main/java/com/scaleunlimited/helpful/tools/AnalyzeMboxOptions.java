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
package com.scaleunlimited.helpful.tools;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

import bixo.config.BixoPlatform.Platform;

public class AnalyzeMboxOptions {
    private boolean _debugLogging = false;

    private String _inputFile;
    private String _outputDir;
    private boolean _localPlatformMode = false;
    

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

    @Option(name = "-localplatform", usage = "Use BixoPlatform in Local mode [optional: default=false]", required = false)
    public void setLocalPlatformMode(boolean mode) {
        _localPlatformMode = mode;
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
    
    public boolean isLocalPlatformMode() {
        return _localPlatformMode;
    }

    public Platform getPlatformMode() {
        if (_localPlatformMode) {
            return Platform.Local;
        }
        return Platform.Hadoop;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
