/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package bixo.tap.jdbc;

import java.io.IOException;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.hsqldb.Server;

/**
 *
 */
public class JDBCTest extends ClusterTestCase
  {
  String inputURLDBFile = "src/test-data/urldb.10.txt.gz";
  String inputContentDBFile = "src/test-data/contentdb.10.txt.gz";

  String outputPath = "build/test-data/jdbctest/";

  private Server server;

  public JDBCTest()
    {
    super( "jbdc tap test", false );
    }

  @Override
  public void setUp() throws IOException
    {
    super.setUp();

    server = new Server();
    server.setDatabasePath( 0, "build/db/testing" );
    server.setDatabaseName( 0, "testing" );
    server.start();
    }

  @Override
  public void tearDown() throws IOException
    {
    super.tearDown();

    server.stop();
    }

  public void testURLDBTap() throws IOException
    {
    Tap source = new Lfs( new TextLine(), inputURLDBFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( URLDBScheme.COLUMN_NAMES ), "\\t" ) );

    parsePipe = new Each( parsePipe, new Identity( URLDBScheme.COLUMN_TYPES ) );

    String url = "jdbc:hsqldb:hsql://localhost/testing";
    String driver = "org.hsqldb.jdbcDriver";

    Tap urldbTap = new URLDBTap( url, driver, SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( getProperties() ).connect( source, urldbTap, parsePipe );

    parseFlow.complete();

    validateLength( parseFlow, 10 );

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), outputPath + "urldb", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( getProperties() ).connect( urldbTap, sink, copyPipe );

    copyFlow.complete();

    validateLength( copyFlow, 10 );
    }

  public void testContentDBTap() throws IOException
    {
    Tap source = new Lfs( new TextLine(), inputContentDBFile );

//    String[] fields = {"url", "fetch_time", "headers", "content"};
    String[] fields = ContentDBScheme.COLUMN_NAMES;
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( fields ), "\\t" ) );

//    String headerDecoder = "new String( org.apache.commons.codec.binary.Base64.decodeBase64( headers.getBytes() ) )";
//    Function headerDecodeFunction = new ExpressionFunction( new Fields( ContentDBScheme.HEADERS_RAW ), headerDecoder, String.class );
//    parsePipe = new Each( parsePipe, new Fields( "headers" ), headerDecodeFunction, Fields.ALL );
//
//    String contentDecode = "new String( org.apache.commons.codec.binary.Base64.decodeBase64( content.getBytes() ) )";
//    Function contentDecodeFunction = new ExpressionFunction( new Fields( ContentDBScheme.CONTENT_RAW ), contentDecode, String.class );
//    parsePipe = new Each( parsePipe, new Fields( "content" ), contentDecodeFunction, Fields.ALL );

    parsePipe = new Each( parsePipe, new Fields( ContentDBScheme.COLUMN_NAMES ), new Identity( ContentDBScheme.COLUMN_TYPES ) );

    String url = "jdbc:hsqldb:hsql://localhost/testing";
    String driver = "org.hsqldb.jdbcDriver";

    Tap contentDBTap = new ContentDBTap( url, driver, SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( getProperties() ).connect( source, contentDBTap, parsePipe );

    parseFlow.complete();

    validateLength( parseFlow, 10 );

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), outputPath + "contentdb", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( getProperties() ).connect( contentDBTap, sink, copyPipe );

    copyFlow.complete();

    validateLength( copyFlow, 10 );
    }

  }
