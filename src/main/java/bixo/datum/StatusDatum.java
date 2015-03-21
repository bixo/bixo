/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.datum;

import com.scaleunlimited.cascading.Payload;
import com.scaleunlimited.cascading.PayloadDatum;

import bixo.exceptions.BaseFetchException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


@SuppressWarnings("serial")
public class StatusDatum extends PayloadDatum {
    
    public static final String URL_FN = fieldName(StatusDatum.class, "url");
    public static final String STATUS_FN = fieldName(StatusDatum.class, "status");
    public static final String HEADERS_FN = fieldName(StatusDatum.class, "headers");
    public static final String EXCEPTION_FN = fieldName(StatusDatum.class, "exception");
    public static final String STATUS_TIME_FN = fieldName(StatusDatum.class, "statusTime");
    public static final String HOST_ADDRESS_FN = fieldName(StatusDatum.class, "hostAddress");
        
    public static final Fields FIELDS = new Fields(URL_FN, STATUS_FN, HEADERS_FN, EXCEPTION_FN, STATUS_TIME_FN, HOST_ADDRESS_FN).append(getSuperFields(StatusDatum.class));

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

    public String getUrl() {
        return _tupleEntry.getString(URL_FN);
    }

    public void setUrl(String url) {
        _tupleEntry.setString(URL_FN, url);
    }

    public UrlStatus getStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(STATUS_FN));
    }

    public void setStatus(UrlStatus status) {
        _tupleEntry.setString(STATUS_FN, status.name());
    }
    
    public HttpHeaders getHeaders() {
        return new HttpHeaders((Tuple)_tupleEntry.getObject(HEADERS_FN));
    }

    public void setHeaders(HttpHeaders headers) {
        if (headers == null) {
            _tupleEntry.setObject(HEADERS_FN, null);
        } else {
            _tupleEntry.setObject(HEADERS_FN, headers.toTuple());
        }
    }
    
    public BaseFetchException getException() {
        return (BaseFetchException)_tupleEntry.getObject(EXCEPTION_FN);
    }

    public void setException(BaseFetchException e) {
        _tupleEntry.setObject(EXCEPTION_FN, e);
    }
    
    public long getStatusTime() {
        return _tupleEntry.getLong(STATUS_TIME_FN);
    }
    
    public void setStatusTime(long statusTime) {
        _tupleEntry.setLong(STATUS_TIME_FN, statusTime);
    }
    
    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }
    
    public void setHostAddress(String hostAddress) {
        _tupleEntry.setString(HOST_ADDRESS_FN, hostAddress);
    }

}
