/*
 * Copyright (c) 2010 TransPac Software, Inc.
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

import com.bixolabs.cascading.Payload;

import bixo.exceptions.BaseFetchException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class StatusDatum extends UrlDatum {
    
    public static final String STATUS_FN = fieldName(StatusDatum.class, "status");
    public static final String HEADERS_FN = fieldName(StatusDatum.class, "headers");
    public static final String EXCEPTION_FN = fieldName(StatusDatum.class, "exception");
    public static final String STATUS_TIME_FN = fieldName(StatusDatum.class, "statusTime");
    public static final String HOST_ADDRESS_FN = fieldName(StatusDatum.class, "hostAddress");
        
    public static final Fields FIELDS = new Fields(STATUS_FN, HEADERS_FN, EXCEPTION_FN, STATUS_TIME_FN, HOST_ADDRESS_FN).append(getSuperFields(StatusDatum.class));

    public StatusDatum() {
        super(FIELDS);
    }
    
    public StatusDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }
    
    /**
     * Constructor for creating StatusDatum for a URL that was fetched successfully.
     * 
     * @param url URL that we fetched.
     * @param headers Headers returned by server.
     * @param hostAddress Host IP address of server.
     * @param payload User-provided payload.
     */
    public StatusDatum(String url, HttpHeaders headers, String hostAddress, Payload payload) {
        this(url, UrlStatus.FETCHED, headers, null, System.currentTimeMillis(), hostAddress, payload);
    }
    
    public StatusDatum(String url, BaseFetchException e, Payload payload) {
        this(url, e.mapToUrlStatus(), null, e, System.currentTimeMillis(), null, payload);
    }
    
    public StatusDatum(String url, UrlStatus status, Payload payload) {
        this(url, status, null, null, System.currentTimeMillis(), null, payload);
    }
    
    public StatusDatum(String url, UrlStatus status, HttpHeaders headers, BaseFetchException e, long statusTime, String hostAddress, Payload payload) {
        super(FIELDS);
        
        setUrl(url);
        setStatus(status);
        setHeaders(headers);
        setException(e);
        setStatusTime(statusTime);
        setHostAddress(hostAddress);
        setPayload(payload);
    }

    public UrlStatus getStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(STATUS_FN));
    }

    public void setStatus(UrlStatus status) {
        _tupleEntry.set(STATUS_FN, status.name());
    }
    
    public HttpHeaders getHeaders() {
        return new HttpHeaders((Tuple)_tupleEntry.get(HEADERS_FN));
    }

    public void setHeaders(HttpHeaders headers) {
        if (headers == null) {
            _tupleEntry.set(HEADERS_FN, null);
        } else {
            _tupleEntry.set(HEADERS_FN, headers.toTuple());
        }
    }
    
    public BaseFetchException getException() {
        return (BaseFetchException)_tupleEntry.getObject(EXCEPTION_FN);
    }

    public void setException(BaseFetchException e) {
        _tupleEntry.set(EXCEPTION_FN, e);
    }
    
    public long getStatusTime() {
        return _tupleEntry.getLong(STATUS_TIME_FN);
    }
    
    public void setStatusTime(long statusTime) {
        _tupleEntry.set(STATUS_TIME_FN, statusTime);
    }
    
    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }
    
    public void setHostAddress(String hostAddress) {
        _tupleEntry.set(HOST_ADDRESS_FN, hostAddress);
    }

    public static Fields getGroupingField() {
        return new Fields(UrlDatum.URL_FN);
    }

}
