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
    
    private String _workingDir = "build/test/working";
    private String _agentName;

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
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
