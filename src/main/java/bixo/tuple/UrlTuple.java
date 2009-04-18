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
package bixo.tuple;

import bixo.IConstants;
import bixo.fetcher.beans.FetchStatusCode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Just a wrapper around tuple with getter and setter for the known fields
 * 
 */
public class UrlTuple extends BaseTuple {

    public static Fields FIELDS = new Fields(IConstants.URL, IConstants.LAST_UPDATED, IConstants.LAST_FETCHED, IConstants.LAST_STATUS);

    public UrlTuple() {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
    }

    public UrlTuple(TupleEntry tupleEntry) {
        super(tupleEntry);
    }

    public UrlTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

    public UrlTuple(String url, long lastUpdated, long lastFetched, FetchStatusCode status) {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
        setUrl(url);
        setLastUpdated(lastUpdated);
        setLastFetched(lastFetched);
        setLastStatus(status);
        
    }

    public String getUrl() {
        return getTupleEntry().getString(IConstants.URL);
    }

    public void setUrl(String url) {
        getTupleEntry().set(IConstants.URL, url);
    }

    public long getLastUpdated() {
        return getTupleEntry().getLong(IConstants.LAST_UPDATED);
    }

    public void setLastUpdated(long timeStamp) {
        getTupleEntry().set(IConstants.LAST_UPDATED, timeStamp);
    }

    public long getLastFetched() {
        return getTupleEntry().getLong(IConstants.LAST_FETCHED);
    }

    public void setLastFetched(long timeStamp) {
        getTupleEntry().set(IConstants.LAST_FETCHED, timeStamp);
    }

    public FetchStatusCode getLastStatus() {
        return FetchStatusCode.fromOrdinal(getTupleEntry().getInteger(IConstants.LAST_STATUS));
    }

    public void setLastStatus(FetchStatusCode statusCode) {
        getTupleEntry().set(IConstants.LAST_STATUS, statusCode.ordinal());
    }

}
