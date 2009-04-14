/*
 * Copyright (c) 2009 Share This, Inc. All rights reserved
 */

package bixo.tap.jdbc;

import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.tap.SinkMode;

/**
 * URL Schema
 * <p/>
 * URL - http://www.mydomain.com/somepage.html?session=yyy
 * Host - mydomain.com
 * Last fetch time - <timestamp> (0 ==> never fetched)
 * Last fetch signature - <MD5> (from content after last fetch)
 * Last changed time - <timestamp>
 * Last status time - <timestamp> (0 ==> no status)
 * Last status - <status code>
 * <p/>
 * The last fetch time is the time when the content was fetched most recently.
 * If a fetch fails, the status time will be updated but not the fetch time.
 * Thus the status time will always be >= the fetch time.
 * <p/>
 * The last changed time is the last time that new content (different signature) was fetched, and is used for adaptive re-fetch.
 * <p/>
 * The <status code> field will be our own enum, not HTTP status codes. There's close to, but not an exact, mapping between the two.
 * <p/>
 * Other data will undoubtedly be added, but this is a start.
 */
public class URLDBTap extends JDBCTap {

    public static final URLTableDesc DEFAULT_TABLE_DESC = new URLTableDesc();

    public static class URLTableDesc extends TableDesc {
        public static final String TABLE_NAME = "urldb";

        public static final String[] COLUMN_NAMES = URLDBScheme.COLUMN_NAMES;

        public static final String[] COLUMN_DEFS = {
                "VARCHAR(100) NOT NULL",  // URL
                "VARCHAR(100) NOT NULL",  // HOST
                "BIGINT",                 // LAST_FETCH_TIME
                "VARCHAR(100)",           // LAST_FETCH_SIGNATURE
                "BIGINT",                 // LAST_CHANGED_TIME
                "BIGINT",                 // LAST_STATUS_TIME
                "INT"                     // LAST_STATUS
        };
        public static final String PRIMARY_KEY = URLDBScheme.URL;

        public URLTableDesc() {
            super(TABLE_NAME, COLUMN_NAMES, COLUMN_DEFS, PRIMARY_KEY);
        }
    }

    public URLDBTap(String connectionUrl, String driverClassName, SinkMode sinkMode) {
        super(connectionUrl, driverClassName, DEFAULT_TABLE_DESC, URLDBScheme.DEFAULT_SCHEME, sinkMode);
    }

    public URLDBTap(String connectionUrl, String driverClassName) {
        super(connectionUrl, driverClassName, DEFAULT_TABLE_DESC, URLDBScheme.DEFAULT_SCHEME);
    }

    public URLDBTap(String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme jdbcScheme, SinkMode sinkMode) {
        super(connectionUrl, driverClassName, tableDesc, jdbcScheme, sinkMode);
    }

    public URLDBTap(String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme jdbcScheme) {
        super(connectionUrl, driverClassName, tableDesc, jdbcScheme);
    }
}
