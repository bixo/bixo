/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo;

import cascading.tuple.Fields;

public interface Constants {

    String URL_DB = "url_db";

    String LINE = "line";

    // url DB
    String URL = "url";
    String LAST_UPDATED = "lastUpdated";
    String LAST_FETCHED = "lastFetched";
    String LAST_STATUS = "lastStatus";

    Fields URL_TUPLE_KEY = new Fields(URL);
    Fields URL_TUPLE_VALUES = new Fields(LAST_UPDATED, LAST_FETCHED, LAST_STATUS);

    String FETCH = "fetch";
    String SCORE = "score";
    String FETCH_STATUS = "status";
    String CONTENT = "content";

}
