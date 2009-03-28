/*
 * Copyright (c) 2009 Share This, Inc. All rights reserved
 */

package sharethis.searchcrawl;

import java.io.IOException;
import java.net.URISyntaxException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  private Options options;

  public static void main( String[] args ) throws URISyntaxException, IOException
    {
    long start = System.currentTimeMillis();

    // env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    Options options = new Options();
    CmdLineParser parser = new CmdLineParser( options );

    try
      {
      parser.parseArgument( args );
      }
    catch( CmdLineException e )
      {
      System.err.println( e.getMessage() );
      printUsageAndExit( parser );
      }

    }


  private static void printUsageAndExit( CmdLineParser parser )
    {
    System.err.println( "java sharethis.logprocessing.Main [options...]" );
    parser.printUsage( System.err );

    System.exit( -1 );
    }

  }