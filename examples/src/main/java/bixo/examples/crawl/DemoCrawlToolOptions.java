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

import com.scaleunlimited.cascading.BasePlatform;

public class DemoCrawlToolOptions extends BaseCrawlToolOptions {

    private boolean _cleanOutputDir = false;
    private boolean _generateHTML = false;
    private int _numReduceTasks = BasePlatform.CLUSTER_REDUCER_COUNT;
    
    @Option(name = "-clean", usage = "Delete the output dir if it exists - WARNING:you won't be prompted!", required = false)
    public void setCleanOutputDir(boolean cleanOutputDir) {
        _cleanOutputDir = cleanOutputDir;
    }

    public boolean isCleanOutputDir() {
        return _cleanOutputDir  ;
    }
    
    @Option(name = "-html", usage = "Generate HTML output as a text file", required = false)
    public void setGenerateHTML(boolean generateHTML) {
        _generateHTML = generateHTML;
    }

    public boolean isGenerateHTML() {
        return _generateHTML  ;
    }
    
    @Option(name = "-numreducetasks", usage = "Number of reduce tasks", required = true)
    public void setNumReduceTasks(int numReduceTasks) {
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
