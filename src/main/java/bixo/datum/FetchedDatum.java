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

import java.io.Serializable;
import java.security.InvalidParameterException;

import com.scaleunlimited.cascading.Payload;
import com.scaleunlimited.cascading.PayloadDatum;


import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class FetchedDatum extends PayloadDatum implements Serializable {
    
    public static final String URL_FN = fieldName(FetchedDatum.class, "url");
    public static final String NEW_BASE_URL_FN = fieldName(FetchedDatum.class, "newBaseUrl");
    public static final String FETCHED_URL_FN = fieldName(FetchedDatum.class, "fetchedUrl");
    public static final String FETCH_TIME_FN = fieldName(FetchedDatum.class, "fetchTime");
    public static final String CONTENT_FN = fieldName(FetchedDatum.class, "content");
    public static final String CONTENT_TYPE_FN = fieldName(FetchedDatum.class, "contentType");
    public static final String RESPONSE_RATE_FN = fieldName(FetchedDatum.class, "responseRate");
    public static final String NUM_REDIRECTS_FN = fieldName(FetchedDatum.class, "numRedirects");
    public static final String HOST_ADDRESS_FN = fieldName(FetchedDatum.class, "hostAddress");
    public static final String HTTP_HEADERS_FN = fieldName(FetchedDatum.class, "httpHeaders");

    public static final Fields FIELDS = new Fields(URL_FN, NEW_BASE_URL_FN,
                    FETCHED_URL_FN, FETCH_TIME_FN, CONTENT_FN, CONTENT_TYPE_FN,
                    RESPONSE_RATE_FN, NUM_REDIRECTS_FN, HOST_ADDRESS_FN,
                    HTTP_HEADERS_FN).append(getSuperFields(FetchedDatum.class));

    public FetchedDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }
    
    public FetchedDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }
    
    public FetchedDatum(String baseUrl, String fetchedUrl, long fetchTime, HttpHeaders headers,
                    ContentBytes content, String contentType, int responseRate) {
        super(FIELDS);

        setUrl(baseUrl);
        setFetchedUrl(fetchedUrl);
        setFetchTime(fetchTime);
        setContent(content);
        setContentType(contentType);
        setResponseRate(responseRate);
        setHeaders(headers);
        
        setNumRedirects(0);
        setNewBaseUrl(null);
    }

    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param url
     *            - Base & redirected url
     * @param payload
     *            - User supplied payload
     */
    public FetchedDatum(String url, Payload payload) {
        this(url, url, 0, new HttpHeaders(), new ContentBytes(), "", 0);
        setPayload(payload);
    }

    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param scoredDatum
     *            Valid datum with url/metadata needed to create FetchedDatum
     */
    public FetchedDatum(final ScoredUrlDatum scoredDatum) {
        // Note: Here we share the payload between the ScoredUrlDatum and the
        // FetchedDatum we're constructing, but we assume noone is modifying
        // this data within the subassembly.
        this(scoredDatum.getUrl(), scoredDatum.getPayload());
    }

    /**
     * Return the original base URL.
     * 
     * @return original URL we tried to fetch
     */
    public String getUrl() {
        
        return _tupleEntry.getString(URL_FN);
    }

    public void setUrl(String baseUrl) {
        if (baseUrl == null) {
            throw new InvalidParameterException("baseUrl cannot be null");
        }

        _tupleEntry.setString(URL_FN, baseUrl);
    }
    
    public String getNewBaseUrl() {
        return _tupleEntry.getString(NEW_BASE_URL_FN);
    }

    public void setNewBaseUrl(String newBaseUrl) {
        _tupleEntry.setString(NEW_BASE_URL_FN, newBaseUrl);
    }

    public String getFetchedUrl() {
        return _tupleEntry.getString(FETCHED_URL_FN);
    }

    public void setFetchedUrl(String fetchedUrl) {
        if (fetchedUrl == null) {
            throw new InvalidParameterException("fetchedUrl cannot be null");
        }

        _tupleEntry.setString(FETCHED_URL_FN, fetchedUrl);
    }
    
    public long getFetchTime() {
        return _tupleEntry.getLong(FETCH_TIME_FN);
    }
    
    public void setFetchTime(long fetchTime) {
        _tupleEntry.setLong(FETCH_TIME_FN, fetchTime);
    }

    public byte[] getContentBytes() {
        return ((ContentBytes)_tupleEntry.getObject(CONTENT_FN)).getBytes();
    }
    
    public int getContentLength() {
        return ((ContentBytes)_tupleEntry.getObject(CONTENT_FN)).getLength();
    }
    
    public void setContent(ContentBytes content) {
        if (content == null) {
            throw new InvalidParameterException("content cannot be null");
        }

        _tupleEntry.setObject(CONTENT_FN, content);
    }
    
    public String getContentType() {
        return _tupleEntry.getString(CONTENT_TYPE_FN);
    }

    public void setContentType(String contentType) {
        if (contentType == null) {
            throw new InvalidParameterException("contentType cannot be null");
        }

        _tupleEntry.setString(CONTENT_TYPE_FN, contentType);
    }
    
    public int getResponseRate() {
        return _tupleEntry.getInteger(RESPONSE_RATE_FN);
    }

    public void setResponseRate(int responseRate) {
        _tupleEntry.setInteger(RESPONSE_RATE_FN, responseRate);
    }
    
    public int getNumRedirects() {
        return _tupleEntry.getInteger(NUM_REDIRECTS_FN);
    }

    public void setNumRedirects(int numRedirects) {
        _tupleEntry.setInteger(NUM_REDIRECTS_FN, numRedirects);
    }

    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }

    public void setHostAddress(String hostAddress) {
        _tupleEntry.setString(HOST_ADDRESS_FN, hostAddress);
    }

    public HttpHeaders getHeaders() {
        return new HttpHeaders((Tuple)_tupleEntry.getObject(HTTP_HEADERS_FN));
    }

    public void setHeaders(HttpHeaders headers) {
        if (headers == null) {
            throw new InvalidParameterException("headers cannot be null");
        }

        _tupleEntry.setObject(HTTP_HEADERS_FN, headers.toTuple());
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[base URL] ");
        result.append(getUrl());
        if (getNewBaseUrl() != null) {
            result.append(" | [perm redir URL] ");
            result.append(getNewBaseUrl());
        }

        if (!getUrl().equals(getFetchedUrl())) {
            result.append(" | [final URL] ");
            result.append(getFetchedUrl());
        }

        HttpHeaders headers = getHeaders();
        for (String headerName : headers.getNames()) {
            result.append(" | [header] ");
            result.append(headerName);
            result.append(": ");
            result.append(headers.getFirst(headerName));
        }

        return result.toString();
    }

}
