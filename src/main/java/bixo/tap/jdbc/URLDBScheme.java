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
public class URLDBScheme extends JDBCScheme
  {
  public static final String URL = "url";
  public static final String HOST = "host";
  public static final String LAST_FETCH_TIME = "last_fetch_time";
  public static final String LAST_FETCH_SIGNATURE = "last_fetch_signature";
  public static final String LAST_CHANGED_TIME = "last_changed_time";
  public static final String LAST_STATUS_TIME = "last_status_time";
  public static final String LAST_STATUS = "last_status";

  public static final String[] COLUMN_NAMES = {URL, HOST, LAST_FETCH_TIME, LAST_FETCH_SIGNATURE, LAST_CHANGED_TIME, LAST_STATUS_TIME, LAST_STATUS};

  public static final URLDBScheme DEFAULT_SCHEME = new URLDBScheme();


  public URLDBScheme()
    {
    super( COLUMN_NAMES, HOST + "," + URL );
    }

  public URLDBScheme( String[] columns, String orderBy )
    {
    super( columns, orderBy );
    }

  public URLDBScheme( String[] columns )
    {
    super( columns );
    }
  }
