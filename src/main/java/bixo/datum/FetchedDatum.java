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

import java.io.Serializable;
import java.security.InvalidParameterException;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class FetchedDatum extends UrlDatum implements Serializable {
    
    private static final String NEW_BASE_URL_FN = fieldName(FetchedDatum.class, "newBaseUrl");
    private static final String FETCHED_URL_FN = fieldName(FetchedDatum.class, "fetchedUrl");
    private static final String FETCH_TIME_FN = fieldName(FetchedDatum.class, "fetchTime");
    private static final String CONTENT_FN = fieldName(FetchedDatum.class, "content");
    private static final String CONTENT_TYPE_FN = fieldName(FetchedDatum.class, "contentType");
    private static final String RESPONSE_RATE_FN = fieldName(FetchedDatum.class, "responseRate");
    private static final String NUM_REDIRECTS_FN = fieldName(FetchedDatum.class, "numRedirects");
    private static final String HOST_ADDRESS_FN = fieldName(FetchedDatum.class, "hostAddress");
    private static final String HTTP_HEADERS_FN = fieldName(FetchedDatum.class, "httpHeaders");

    public static final Fields FIELDS = new Fields(NEW_BASE_URL_FN,
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

        setBaseUrl(baseUrl);
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
     * Return the original URL - use the UrlDatum support for this.
     * 
     * @return original URL we tried to fetch
     */
    public String getBaseUrl() {
        return getUrl();
    }

    public void setBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            throw new InvalidParameterException("baseUrl cannot be null");
        }

        setUrl(baseUrl);
    }
    
    public String getNewBaseUrl() {
        return _tupleEntry.getString(NEW_BASE_URL_FN);
    }

    public void setNewBaseUrl(String newBaseUrl) {
        _tupleEntry.set(NEW_BASE_URL_FN, newBaseUrl);
    }

    public String getFetchedUrl() {
        return _tupleEntry.getString(FETCHED_URL_FN);
    }

    public void setFetchedUrl(String fetchedUrl) {
        if (fetchedUrl == null) {
            throw new InvalidParameterException("fetchedUrl cannot be null");
        }

        _tupleEntry.set(FETCHED_URL_FN, fetchedUrl);
    }
    
    public long getFetchTime() {
        return _tupleEntry.getLong(FETCH_TIME_FN);
    }
    
    public void setFetchTime(long fetchTime) {
        _tupleEntry.set(FETCH_TIME_FN, fetchTime);
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

        _tupleEntry.set(CONTENT_FN, content);
    }
    
    public String getContentType() {
        return _tupleEntry.getString(CONTENT_TYPE_FN);
    }

    public void setContentType(String contentType) {
        if (contentType == null) {
            throw new InvalidParameterException("contentType cannot be null");
        }

        _tupleEntry.set(CONTENT_TYPE_FN, contentType);
    }
    
    public int getResponseRate() {
        return _tupleEntry.getInteger(RESPONSE_RATE_FN);
    }

    public void setResponseRate(int responseRate) {
        _tupleEntry.set(RESPONSE_RATE_FN, responseRate);
    }
    
    public int getNumRedirects() {
        return _tupleEntry.getInteger(NUM_REDIRECTS_FN);
    }

    public void setNumRedirects(int numRedirects) {
        _tupleEntry.set(NUM_REDIRECTS_FN, numRedirects);
    }

    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }

    public void setHostAddress(String hostAddress) {
        _tupleEntry.set(HOST_ADDRESS_FN, hostAddress);
    }

    public HttpHeaders getHeaders() {
        return new HttpHeaders((Tuple)_tupleEntry.get(HTTP_HEADERS_FN));
    }

    public void setHeaders(HttpHeaders headers) {
        if (headers == null) {
            throw new InvalidParameterException("headers cannot be null");
        }

        _tupleEntry.set(HTTP_HEADERS_FN, headers.toTuple());
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[base URL] ");
        result.append(getBaseUrl());
        if (getNewBaseUrl() != null) {
            result.append(" | [perm redir URL] ");
            result.append(getNewBaseUrl());
        }

        if (!getBaseUrl().equals(getFetchedUrl())) {
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
