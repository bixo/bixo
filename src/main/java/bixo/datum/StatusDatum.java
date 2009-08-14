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

import bixo.exceptions.BaseFetchException;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("unchecked")
public class StatusDatum extends BaseDatum {
    private String _url;
    private UrlStatus _status;
    private HttpHeaders _headers;
    private BaseFetchException _exception;
    
    /**
     * Constructor for creating StatusDatum for a URL that was fetched successfully.
     * 
     * @param url URL that we fetched.
     * @param headers Headers returned by server.
     * @param metaData User-provided meta-data.
     */
    public StatusDatum(String url, HttpHeaders headers, Map<String, Comparable> metaData) {
        this(url, UrlStatus.FETCHED, headers, null, metaData);
    }
    
    public StatusDatum(String url, BaseFetchException e, Map<String, Comparable> metaData) {
        this(url, e.mapToUrlStatus(), null, e, metaData);
    }
    
    @SuppressWarnings("unchecked")
    public StatusDatum(String url, UrlStatus status, HttpHeaders headers, BaseFetchException e, Map<String, Comparable> metaData) {
        super(metaData);
        
        _url = url;
        _status = status;
        _headers = headers;
        _exception = e;
    }

    public String getUrl() {
        return _url;
    }

    public UrlStatus getStatus() {
        return _status;
    }

    public HttpHeaders getHeaders() {
        return _headers;
    }

    public BaseFetchException getException() {
        return _exception;
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String URL_FIELD = fieldName(StatusDatum.class, "url");
    public static final String STATUS_FIELD = fieldName(StatusDatum.class, "status");
    public static final String HEADERS_FIELD = fieldName(StatusDatum.class, "headers");
    public static final String EXCEPTION_FIELD = fieldName(StatusDatum.class, "exception");
        
    public static final Fields FIELDS = new Fields(URL_FIELD, STATUS_FIELD, HEADERS_FIELD, EXCEPTION_FIELD);
    
    public StatusDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        initFromTupleEntry(new TupleEntry(getStandardFields(), tuple));
    }
    
    public StatusDatum(TupleEntry entry, Fields metaDataFields) {
        super(entry.getTuple(), metaDataFields);
        initFromTupleEntry(entry);
    }
    
    private void initFromTupleEntry(TupleEntry entry) {
        _url = entry.getString(URL_FIELD);
        _status = UrlStatus.valueOf(entry.getString(STATUS_FIELD));
        _headers = new HttpHeaders(entry.getString(HEADERS_FIELD));
        _exception = (BaseFetchException)entry.get(EXCEPTION_FIELD);
    }
    
    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _url, _status.name(), _headers == null ? null : _headers.toString(), (Comparable)_exception };
    }


}
