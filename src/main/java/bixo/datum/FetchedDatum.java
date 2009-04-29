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

import org.apache.hadoop.io.BytesWritable;

import bixo.IConstants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FetchedDatum extends BaseDatum {

    private final FetchStatusCode _statusCode;
    private final String _baseUrl;
    private final String _fetchedUrl;
    private final long _fetchTime;
    private final BytesWritable _content;
    private final String _contentType;
    private final int _responseRate;

    @SuppressWarnings("unchecked")
    public FetchedDatum(FetchStatusCode statusCode, String url, String redirectedUrl, long fetchTime, BytesWritable content, String contentType, int responseRate, Map<String, Comparable> metaData) {
        super(metaData);
        
        _statusCode = statusCode;
        _baseUrl = url;
        _fetchedUrl = redirectedUrl;
        _fetchTime = fetchTime;
        _content = content;
        _contentType = contentType;
        _responseRate = responseRate;
    }

    public FetchStatusCode getStatusCode() {
        return _statusCode;
    }

    public String getBaseUrl() {
        return _baseUrl;
    }

    public String getFetchedUrl() {
        return _fetchedUrl;
    }

    public long getFetchTime() {
        return _fetchTime;
    }

    public BytesWritable getContent() {
        return _content;
    }

    public String getContentType() {
        return _contentType;
    }

    public int getResponseRate() {
        return _responseRate;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getValues() {
        return new Comparable[] { _statusCode, _baseUrl, _fetchedUrl, _fetchTime, _content, _contentType, _responseRate };
    }

    public static FetchedDatum fromTuple(Tuple tuple, Fields metaDataFieldNames) {

        TupleEntry entry = new TupleEntry(getFields(), tuple);
        FetchStatusCode fetchStatus = FetchStatusCode.fromOrdinal(entry.getInteger(IConstants.FETCH_STATUS));
        String baseUrl = entry.getString(IConstants.BASE_URL);
        String fetchedUrl = entry.getString(IConstants.FETECHED_URL);
        long fetchTime = entry.getLong(IConstants.FETCH_TIME);
        BytesWritable content = (BytesWritable)entry.get(IConstants.CONTENT);
        String contentType = entry.getString(IConstants.CONTENT_TYPE);
        int responseRate = entry.getInteger(IConstants.FETCH_RATE);
        
        return new FetchedDatum(fetchStatus, baseUrl, fetchedUrl, fetchTime, content, contentType, responseRate, BaseDatum.extractMetaData(tuple, getFields().size(), metaDataFieldNames));
    }

    public static Fields getFields() {
        return new Fields(IConstants.FETCH_STATUS, IConstants.BASE_URL, IConstants.FETECHED_URL, IConstants.FETCH_TIME, IConstants.CONTENT, IConstants.CONTENT_TYPE, IConstants.FETCH_RATE);
    }

}
