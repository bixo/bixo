/*
 * Copyright 2009-2012 Scale Unlimited
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

public class ExportToolOptions {

    public static final int DEFAULT_MAX_THREADS = 10;

    private String _crawlDir;
    private String _outputDir;
    private boolean _overWrite = false;
    private int _maxThreads = DEFAULT_MAX_THREADS;
    private int _titleWeight = 1;

    @Option(name = "-crawldir", usage = "location of the bixo crawl loops (ALL! will be munged into mahout sequencefiles)", required = true)
    public void setCrawlDir(String crawlDir) {
        _crawlDir = crawlDir;
    }

    @Option(name = "-outputdir", usage = "output directory for mahout sequence files", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    @Option(name = "-ow", usage = "over write contents of output directory", required = false)
    public void setOverwrite(boolean overWrite) {
        _overWrite = overWrite;
    }

    @Option(name = "-titleweight", usage = "number of times to duplicate the title in the seq files to boost its importance (default = 1)", required = false)
    public void setNumLoops(int titleWeight) {
        _titleWeight = titleWeight;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public String getOutputDir() {
        return _outputDir;
    }

    public String getCrawlDir() {
        return _crawlDir;
    }

    public boolean getOverWrite() {
        return _overWrite;
    }

}
