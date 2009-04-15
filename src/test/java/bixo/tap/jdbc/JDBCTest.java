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

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
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

import java.io.IOException;

/**
 *
 */
public class JDBCTest extends ClusterTestCase {
    String inputURLDBFile = "src/test-data/urldb.10.txt.gz";
    String inputContentDBFile = "src/test-data/contentdb.10.txt.gz";

    String outputPath = "build/test-data/jdbctest/";

    private Server server;

    public JDBCTest() {
        super("jbdc tap test", false);
    }

    @Override
    public void setUp() throws IOException {
        super.setUp();

        server = new Server();
        server.setDatabasePath(0, "build/db/testing");
        server.setDatabaseName(0, "testing");
        server.start();
    }

    @Override
    public void tearDown() throws IOException {
        super.tearDown();

        try {
            server.stop();
        } catch (Exception e) {
            // ignore
        }
    }

    public void testURLDBTap() throws IOException {
        Tap source = new Lfs(new TextLine(), inputURLDBFile);

        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(new Fields(SimpleURLTableDesc.COLUMN_NAMES), "\\t"));

        parsePipe = new Each(parsePipe, new Identity(SimpleURLTableDesc.COLUMN_TYPES));

        String url = "jdbc:hsqldb:hsql://localhost/testing";
        String driver = "org.hsqldb.jdbcDriver";

        String sortBy = SimpleURLTableDesc.HOST + "," + SimpleURLTableDesc.URL;
        JDBCScheme urldbScheme = new JDBCScheme(SimpleURLTableDesc.COLUMN_NAMES, sortBy);
        Tap urldbTap = new JDBCTap(url, driver, new SimpleURLTableDesc(), urldbScheme, SinkMode.REPLACE);

        Flow parseFlow = new FlowConnector(getProperties()).connect(source, urldbTap, parsePipe);

        parseFlow.complete();

        validateLength(parseFlow, 10);

        Tap sink = new Lfs(new TextLine(), outputPath + "urldb", SinkMode.REPLACE);

        Pipe copyPipe = new Each("read", new Identity());

        Flow copyFlow = new FlowConnector(getProperties()).connect(urldbTap, sink, copyPipe);

        copyFlow.complete();

        validateLength(copyFlow, 10);
    }

    public void testContentDBTap() throws IOException {
        Tap source = new Lfs(new TextLine(), inputContentDBFile);

        String[] fields = SimpleContentTableDesc.COLUMN_NAMES;
        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(new Fields(fields), "\\t"));

        parsePipe = new Each(parsePipe, new Fields(SimpleContentTableDesc.COLUMN_NAMES), new Identity(SimpleContentTableDesc.COLUMN_TYPES));

        String url = "jdbc:hsqldb:hsql://localhost/testing";
        String driver = "org.hsqldb.jdbcDriver";

        String sortBy = SimpleContentTableDesc.URL + ", " + SimpleContentTableDesc.FETCH_TIME;
        JDBCScheme contentDBScheme = new JDBCScheme(SimpleContentTableDesc.COLUMN_NAMES, sortBy);
        Tap contentDBTap = new JDBCTap(url, driver, new SimpleContentTableDesc(), contentDBScheme, SinkMode.REPLACE);

        Flow parseFlow = new FlowConnector(getProperties()).connect(source, contentDBTap, parsePipe);

        parseFlow.complete();

        validateLength(parseFlow, 10);

        Tap sink = new Lfs(new TextLine(), outputPath + "contentdb", SinkMode.REPLACE);

        Pipe copyPipe = new Each("read", new Identity());

        Flow copyFlow = new FlowConnector(getProperties()).connect(contentDBTap, sink, copyPipe);

        copyFlow.complete();

        validateLength(copyFlow, 10);
    }

}
