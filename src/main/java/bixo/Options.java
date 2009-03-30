/*
 * Copyright (c) 2009 The Bixo Project. All rights reserved
 */

package bixo;

import java.net.URI;

import org.kohsuke.args4j.Option;

/**
 *
 */
public class Options
  {
  boolean debugLogging = false;

  public boolean isDebugLogging()
    {
    return debugLogging;
    }

  @Option(name = "-d", usage = "debug logging", required = false)
  public void setDebugLogging( boolean debugLogging )
    {
    this.debugLogging = debugLogging;
    }
  }