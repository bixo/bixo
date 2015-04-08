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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

import com.scaleunlimited.cascading.BasePlatform;

import bixo.config.BixoPlatform.Platform;

public class DemoWebMiningOptions {
    
    private String _workingDir = "build/test/working";
    private String _agentName;
    private boolean _localPlatformMode;
    private int _numReduceTasks = BasePlatform.CLUSTER_REDUCER_COUNT;

    @Option(name = "-workingdir", usage = "path to directory for fetching", required = false)
    public void setWorkingDir(String workingDir) {
        _workingDir = workingDir;
    }

    public String getWorkingDir() {
        return _workingDir;
    }

    @Option(name = "-agentname", usage = "user agent name", required = true)
    public void setAgentName(String agentName) {
        _agentName = agentName;
    }

    public String getAgentName() {
        return _agentName;
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

    @Option(name = "-numreducetasks", usage = "Number of reduce tasks", required = true)
    public void setNumReducers(int numReduceTasks) {
        _numReduceTasks  = numReduceTasks;
    }

    public int getNumReduceTasks() {
        return _numReduceTasks;
    }
    

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
