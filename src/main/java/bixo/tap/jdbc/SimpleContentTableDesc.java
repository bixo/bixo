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
public class SimpleContentTableDesc extends TableDesc {

    public static final String URL = "url";
    public static final String FETCH_TIME = "fetch_time";
    public static final String HEADERS_RAW = "headers_raw";
    public static final String CONTENT_RAW = "content_raw";

    public static final String[] COLUMN_NAMES = {URL, FETCH_TIME, HEADERS_RAW, CONTENT_RAW};

    public static final Class[] COLUMN_TYPES = {String.class, long.class, String.class, String.class};

    public static final String TABLE_NAME = "contentdb";

    public static final String[] COLUMN_DEFS = {
            "VARCHAR(100) NOT NULL",  // URL
            "BIGINT",                 // FETCH_TIME
            "VARCHAR(100)",           // HEADERS_RAW
            "VARCHAR(100)"            // CONTENT_RAW
    };

    public static final String PRIMARY_KEY = SimpleURLTableDesc.URL;

    public SimpleContentTableDesc() {
        super(TABLE_NAME, COLUMN_NAMES, COLUMN_DEFS, PRIMARY_KEY);
    }
}
