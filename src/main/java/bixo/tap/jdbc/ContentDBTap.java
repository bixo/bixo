/*
 * Copyright (c) 2009 Share This, Inc. All rights reserved
 */

package bixo.tap.jdbc;

import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.tap.SinkMode;

/**
 * Content Schema
 * <p/>
 * URL - http://www.mydomain.com/somepage.html?session=yyy
 * Fetch time - <timestamp>
 * Headers - Map<Key, Value> (strings from HTTP response)
 * Content - byte[]
 * <p/>
 * This is the "raw" content. As such, it doesn't have a charset field, as that would be derived during the parse process (ie using http headers and potentially values from inside of the content).
 * <p/>
 * I think I'd want to add a download speed field, so that we could tune fetches by server responsiveness, but that's a future.
 */
public class ContentDBTap extends JDBCTap
  {

  public static final ContentTableDesc DEFAULT_TABLE_DESC = new ContentTableDesc();

  public static class ContentTableDesc extends TableDesc
    {
    public static final String TABLE_NAME = "contentdb";

    public static final String[] COLUMN_NAMES = ContentDBScheme.COLUMN_NAMES;

    public static final String[] COLUMN_DEFS = {"VARCHAR(100) NOT NULL",  // URL
                                                "BIGINT",                 // FETCH_TIME
                                                "VARCHAR(100)",           // HEADERS_RAW
                                                "VARCHAR(100)"            // CONTENT_RAW
    };
    public static final String PRIMARY_KEY = URLDBScheme.URL;

    public ContentTableDesc()
      {
      super( TABLE_NAME, COLUMN_NAMES, COLUMN_DEFS, PRIMARY_KEY );
      }
    }

  public ContentDBTap( String connectionUrl, String driverClassName, SinkMode sinkMode )
    {
    super( connectionUrl, driverClassName, DEFAULT_TABLE_DESC, ContentDBScheme.DEFAULT_SCHEME, sinkMode );
    }

  public ContentDBTap( String connectionUrl, String driverClassName )
    {
    super( connectionUrl, driverClassName, DEFAULT_TABLE_DESC, ContentDBScheme.DEFAULT_SCHEME );
    }

  public ContentDBTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme jdbcScheme, SinkMode sinkMode )
    {
    super( connectionUrl, driverClassName, tableDesc, jdbcScheme, sinkMode );
    }

  public ContentDBTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme jdbcScheme )
    {
    super( connectionUrl, driverClassName, tableDesc, jdbcScheme );
    }
  }