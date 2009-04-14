/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package bixo.tap.jdbc;

import cascading.jdbc.JDBCScheme;

/**
 *
 */
public class ContentDBScheme extends JDBCScheme
  {
  public static final String URL = "url";
  public static final String FETCH_TIME = "fetch_time";
  public static final String HEADERS_RAW = "headers_raw";
  public static final String CONTENT_RAW = "content_raw";

  public static final String[] COLUMN_NAMES = {URL, FETCH_TIME, HEADERS_RAW, CONTENT_RAW};

  public static final Class[] COLUMN_TYPES = {String.class, long.class, String.class, String.class};

  public static final ContentDBScheme DEFAULT_SCHEME = new ContentDBScheme();


  public ContentDBScheme()
    {
    super( COLUMN_NAMES, URL + ", " + FETCH_TIME );
    }

  public ContentDBScheme( String[] columns, String orderBy )
    {
    super( columns, orderBy );
    }

  public ContentDBScheme( String[] columns )
    {
    super( columns );
    }
  }