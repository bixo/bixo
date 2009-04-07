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
package bixo.items;

import bixo.Constants;
import bixo.fetcher.FetchStatus;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Just a wrapper around tuple with getter and setter for the known fields
 * 
 */
public class UrlItem {

    private final Tuple _tuple;
    public static Fields FIELDS = new Fields(Constants.URL, Constants.LAST_UPDATED, Constants.LAST_FETCHED, Constants.LAST_STATUS);

    public UrlItem(String url, long lastUpdated, long lastFetched, FetchStatus status) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(lastUpdated);
        _tuple.add(lastFetched);
        _tuple.add(status.ordinal());
    }

    public UrlItem(Tuple tuple) {
        _tuple = tuple;
    }

    public String getUrl() {
        return _tuple.getString(FIELDS.getPos(Constants.URL));
    }

    public long getLastFetched() {
        return _tuple.getLong(FIELDS.getPos(Constants.LAST_FETCHED));
    }

    public Tuple toTuple() {
        return _tuple;
    }
}
