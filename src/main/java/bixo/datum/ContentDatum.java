/*
 * Copyright (c) 2010-2011 TransPac Software, Inc.
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

import com.scaleunlimited.cascading.Payload;
import com.scaleunlimited.cascading.PayloadDatum;


import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ContentDatum extends PayloadDatum implements Serializable {
    public static final String BASE_URL_FN = fieldName(ContentDatum.class, "baseUrl");
    public static final String FETCHED_URL_FN = fieldName(ContentDatum.class, "fetchedUrl");
    public static final String CONTENT_FN = fieldName(ContentDatum.class, "content");
    public static final String CONTENT_TYPE_FN = fieldName(ContentDatum.class, "contentType");
    public static final String HTTP_HEADERS_FN = fieldName(ContentDatum.class, "httpHeaders");
    public static final String HOST_ADDRESS_FN = fieldName(ContentDatum.class, "hostAddress");

    public static final Fields FIELDS = new Fields(BASE_URL_FN, FETCHED_URL_FN, CONTENT_FN, CONTENT_TYPE_FN, HTTP_HEADERS_FN, HOST_ADDRESS_FN);

    public ContentDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }
    
    public ContentDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }
    
    public ContentDatum(String baseUrl, String fetchedUrl, HttpHeaders headers, ContentBytes content, String contentType) {
        super(FIELDS);

        setBaseUrl(baseUrl);
        setFetchedUrl(fetchedUrl);
        setContent(content);
        setContentType(contentType);
        setHeaders(headers);
    }

    /**
     * Create place-holder ContentDatum from the data used to attempt the fetch.
     * 
     * @param url
     *            - Base & redirected url
     * @param payload
     *            - User supplied payload
     */
    public ContentDatum(String url, Payload payload) {
        this(url, url, new HttpHeaders(), new ContentBytes(), "");
        setPayload(payload);
    }

    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param scoredDatum
     *            Valid datum with url/metadata needed to create FetchedDatum
     */
    public ContentDatum(final ScoredUrlDatum scoredDatum) {
        // Note: Here we share the payload between the ScoredUrlDatum and the
        // FetchedDatum we're constructing, but we assume no one is modifying
        // this data within the sub-assembly.
        this(scoredDatum.getUrl(), scoredDatum.getPayload());
    }

    /**
     * Return the original URL - use the UrlDatum support for this.
     * 
     * @return original URL we tried to fetch
     */
    public String getBaseUrl() {
        return _tupleEntry.getString(BASE_URL_FN);
    }

    public void setBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            throw new InvalidParameterException("baseUrl cannot be null");
        }

        _tupleEntry.setString(BASE_URL_FN, baseUrl);
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
    
    public HttpHeaders getHeaders() {
        return new HttpHeaders((Tuple)_tupleEntry.getObject(HTTP_HEADERS_FN));
    }

    public void setHeaders(HttpHeaders headers) {
        if (headers == null) {
            throw new InvalidParameterException("headers cannot be null");
        }

        _tupleEntry.setObject(HTTP_HEADERS_FN, headers.toTuple());
    }

    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }

    public void setHostAddress(String hostAddress) {
        _tupleEntry.setString(HOST_ADDRESS_FN, hostAddress);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[base URL] ");
        result.append(getBaseUrl());

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
