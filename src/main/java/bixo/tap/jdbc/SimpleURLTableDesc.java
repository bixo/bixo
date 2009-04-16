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

import cascading.jdbc.TableDesc;

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
public class SimpleURLTableDesc extends TableDesc {

    public static final String URL = "url";
    public static final String HOST = "host";
    public static final String LAST_FETCH_TIME = "last_fetch_time";
    public static final String LAST_FETCH_SIGNATURE = "last_fetch_signature";
    public static final String LAST_CHANGED_TIME = "last_changed_time";
    public static final String LAST_STATUS_TIME = "last_status_time";
    public static final String LAST_STATUS = "last_status";

    public static final String[] COLUMN_NAMES = {URL, HOST, LAST_FETCH_TIME, LAST_FETCH_SIGNATURE, LAST_CHANGED_TIME, LAST_STATUS_TIME, LAST_STATUS};

    public static final Class[] COLUMN_TYPES = {String.class, String.class, long.class, String.class, long.class, long.class, int.class};

    public static final String TABLE_NAME = "urldb";

    public static final String[] COLUMN_DEFS = {
            "VARCHAR NOT NULL",  // URL
            "VARCHAR NOT NULL",  // HOST
            "BIGINT",            // LAST_FETCH_TIME
            "VARCHAR",           // LAST_FETCH_SIGNATURE
            "BIGINT",            // LAST_CHANGED_TIME
            "BIGINT",            // LAST_STATUS_TIME
            "INT"                // LAST_STATUS
    };
    public static final String PRIMARY_KEY = URL;

    public SimpleURLTableDesc() {
        super(TABLE_NAME, COLUMN_NAMES, COLUMN_DEFS, PRIMARY_KEY);
    }
}
