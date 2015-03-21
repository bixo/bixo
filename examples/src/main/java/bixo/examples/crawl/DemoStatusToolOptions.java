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
package bixo.examples.crawl;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

import bixo.config.BixoPlatform.Platform;

public class DemoStatusToolOptions {
    private boolean _debugLogging = false;
    private boolean _exportDb = false;
    private String _workingDir;
    private boolean _localPlatformMode;
    
    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        _debugLogging = debugLogging;
    }

    public boolean isDebugLogging() {
        return _debugLogging;
    }
    
    @Option(name = "-workingdir", usage = "Directory of preceeding crawl", required = true)
    public void setWorkingDir(String workingDir) {
    	_workingDir = workingDir;
    }
    
    public String getWorkingDir() {
        return _workingDir;
    }

    @Option(name = "-exportdb", usage = "only export the crawldb", required = false)
    public void setExportDB(boolean exportDb) {
        _exportDb = exportDb;
    }

    public boolean isExportDb() {
        return _exportDb;
    }
    
    @Option(name = "-localplatform", usage = "Use BixoPlatform in Local mode [optional: default=false]", required = false)
    public void setLocalPlatformMode(boolean mode) {
        _localPlatformMode = mode;
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
