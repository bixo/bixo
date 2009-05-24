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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FetchedDatum extends BaseDatum {

    private FetchStatusCode _statusCode;
    private String _baseUrl;
    private String _fetchedUrl;
    private long _fetchTime;
    private BytesWritable _content;
    private String _contentType;
    private int _responseRate;

    @SuppressWarnings("unchecked")
    public FetchedDatum(FetchStatusCode statusCode, String baseUrl, String redirectedUrl, long fetchTime, BytesWritable content, String contentType, int responseRate, Map<String, Comparable> metaData) {
        super(metaData);
        
        _statusCode = statusCode;
        _baseUrl = baseUrl;
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

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String STATUS_CODE_FIELD = fieldName(FetchedDatum.class, "statusCode");
    public static final String BASE_URL_FIELD = fieldName(FetchedDatum.class, "baseUrl");
    public static final String FETCHED_URL_FIELD = fieldName(FetchedDatum.class, "fetchedUrl");
    public static final String FETCH_TIME_FIELD = fieldName(FetchedDatum.class, "fetchTime");
    public static final String CONTENT_FIELD = fieldName(FetchedDatum.class, "content");
    public static final String CONTENT_TYPE_FIELD = fieldName(FetchedDatum.class, "contentType");
    public static final String RESPONSE_RATE_FIELD = fieldName(FetchedDatum.class, "responseRate");

    public static final Fields FIELDS = new Fields(STATUS_CODE_FIELD, BASE_URL_FIELD, FETCHED_URL_FIELD, FETCH_TIME_FIELD, CONTENT_FIELD, CONTENT_TYPE_FIELD, RESPONSE_RATE_FIELD);

    public FetchedDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        initFromTupleEntry(new TupleEntry(getStandardFields(), tuple));
    }
    
    public FetchedDatum(TupleEntry entry, Fields metaDataFields) {
        super(entry.getTuple(), metaDataFields);
        initFromTupleEntry(entry);
    }

    private void initFromTupleEntry(TupleEntry entry) {
        _statusCode = FetchStatusCode.fromOrdinal(entry.getInteger(STATUS_CODE_FIELD));
        _baseUrl = entry.getString(BASE_URL_FIELD);
        _fetchedUrl = entry.getString(FETCHED_URL_FIELD);
        _fetchTime = entry.getLong(FETCH_TIME_FIELD);
        _content = (BytesWritable)entry.get(CONTENT_FIELD);
        _contentType = entry.getString(CONTENT_TYPE_FIELD);
        _responseRate = entry.getInteger(RESPONSE_RATE_FIELD);
    }
    
    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _statusCode.ordinal(), _baseUrl, _fetchedUrl, _fetchTime, _content, _contentType, _responseRate };
    }

}
