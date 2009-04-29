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
package bixo.datum;

import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UrlDatum extends BaseDatum {

    String _url;
    long _lastUpdated;
    long _lastFetched;
    FetchStatusCode _lastStatus;

    @SuppressWarnings("unchecked")
    public UrlDatum(String url, long lastFetched, long lastUpdated, FetchStatusCode lastStatus, Map<String, Comparable> metaData) {
        super(metaData);
        _url = url;
        _lastFetched = lastFetched;
        _lastUpdated = lastUpdated;
        _lastStatus = lastStatus;

    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public long getLastUpdated() {
        return _lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        _lastUpdated = lastUpdated;
    }

    public long getLastFetched() {
        return _lastFetched;
    }

    public void setLastFetched(long lastFetched) {
        _lastFetched = lastFetched;
    }

    public FetchStatusCode getLastStatus() {
        return _lastStatus;
    }

    public void setLastStatus(FetchStatusCode lastStatus) {
        _lastStatus = lastStatus;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getValues() {
        return new Comparable[] { _url, _lastUpdated, _lastFetched, _lastStatus.ordinal() };
    }

    public static Fields getFields() {
        return new Fields(IFieldNames.SOURCE_URL, IFieldNames.SOURCE_LAST_UPDATED, IFieldNames.SOURCE_LAST_FETCHED, IFieldNames.SOURCE_FETCH_STATUS);
    }

    public static UrlDatum fromTuple(Tuple tuple, Fields metaDataFieldNames) {
        TupleEntry entry = new TupleEntry(getFields(), tuple);
        String url = entry.getString(IFieldNames.SOURCE_URL);
        long lastFetched = entry.getLong(IFieldNames.SOURCE_LAST_FETCHED);
        long lastUpdated = entry.getLong(IFieldNames.SOURCE_LAST_UPDATED);
        FetchStatusCode fetchStatus = FetchStatusCode.fromOrdinal(entry.getInteger(IFieldNames.SOURCE_FETCH_STATUS));

        return new UrlDatum(url, lastFetched, lastUpdated, fetchStatus, BaseDatum.extractMetaData(tuple, getFields().size(), metaDataFieldNames));
    }

}
