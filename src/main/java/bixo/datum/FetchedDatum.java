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

import java.security.InvalidParameterException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FetchedDatum extends BaseDatum {
    private String _baseUrl;
    private String _newBaseUrl;
    private String _fetchedUrl;
    private long _fetchTime;
    private BytesWritable _content;
    private String _contentType;
    private int _responseRate;
    private int _numRedirects;
    private HttpHeaders _headers;

    @SuppressWarnings("unchecked")
    public FetchedDatum(String baseUrl, String redirectedUrl, long fetchTime, HttpHeaders headers,
                    BytesWritable content, String contentType, int responseRate,
                    Map<String, Comparable> metaData) {
        super(metaData);

        if (baseUrl == null) {
            throw new InvalidParameterException("baseUrl cannot be null");
        }

        if (redirectedUrl == null) {
            throw new InvalidParameterException("redirectedUrl cannot be null");
        }

        if (headers == null) {
            throw new InvalidParameterException("headers cannot be null");
        }

        if (content == null) {
            throw new InvalidParameterException("content cannot be null");
        }

        if (contentType == null) {
            throw new InvalidParameterException("contentType cannot be null");
        }

        _baseUrl = baseUrl;
        _newBaseUrl = null;
        _fetchedUrl = redirectedUrl;
        _fetchTime = fetchTime;
        _content = content;
        _contentType = contentType;
        _responseRate = responseRate;
        _numRedirects = 0;
        _headers = headers;
    }

    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param url
     *            - Base & redirected url
     * @param metaData
     *            - metadata
     */
    @SuppressWarnings("unchecked")
    public FetchedDatum(String url, Map<String, Comparable> metaData) {
        this(url, url, 0, new HttpHeaders(), new BytesWritable(), "", 0, metaData);
    }

    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param scoredDatum
     *            Valid datum with url/metadata needed to create FetchedDatum
     */
    public FetchedDatum(final ScoredUrlDatum scoredDatum) {
        this(scoredDatum.getUrl(), scoredDatum.getMetaDataMap());
    }

    public String getBaseUrl() {
        return _baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            throw new InvalidParameterException("baseUrl cannot be null");
        }

        _baseUrl = baseUrl;
    }
    
    public String getNewBaseUrl() {
        return _newBaseUrl;
    }

    public void setNewBaseUrl(String newBaseUrl) {
        _newBaseUrl = newBaseUrl;
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

    // Helper methods to mask Hadoop 0.18.3 and 0.19+ delta
    @SuppressWarnings("deprecation")
    public byte[] getContentBytes() {
        return _content.get();
    }
    
    @SuppressWarnings("deprecation")
    public int getContentLength() {
        return _content.getSize();
    }
    
    public String getContentType() {
        return _contentType;
    }

    public int getResponseRate() {
        return _responseRate;
    }

    public int getNumRedirects() {
        return _numRedirects;
    }

    public void setNumRedirects(int numRedirects) {
        _numRedirects = numRedirects;
    }

    public HttpHeaders getHeaders() {
        return _headers;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[base URL] ");
        result.append(_baseUrl);
        if (_newBaseUrl != null) {
            result.append(" | [perm redir URL] ");
            result.append(_newBaseUrl);
        }

        if (!_baseUrl.equals(_fetchedUrl)) {
            result.append(" | [final URL] ");
            result.append(_fetchedUrl);
        }

        if (_headers != null) {
            for (String headerName : _headers.getNames()) {
                result.append(" | [header] ");
                result.append(headerName);
                result.append(": ");
                result.append(_headers.getFirst(headerName));
            }
        }

        return result.toString();
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================

    // Cascading field names that correspond to the datum fields.
    public static final String BASE_URL_FIELD = fieldName(FetchedDatum.class, "baseUrl");
    public static final String NEW_BASE_URL_FIELD = fieldName(FetchedDatum.class, "newBaseUrl");
    public static final String FETCHED_URL_FIELD = fieldName(FetchedDatum.class, "fetchedUrl");
    public static final String FETCH_TIME_FIELD = fieldName(FetchedDatum.class, "fetchTime");
    public static final String CONTENT_FIELD = fieldName(FetchedDatum.class, "content");
    public static final String CONTENT_TYPE_FIELD = fieldName(FetchedDatum.class, "contentType");
    public static final String RESPONSE_RATE_FIELD = fieldName(FetchedDatum.class, "responseRate");
    public static final String NUM_REDIRECTS_FIELD = fieldName(FetchedDatum.class, "numRedirects");
    public static final String HTTP_HEADERS_FIELD = fieldName(FetchedDatum.class, "httpHeaders");

    public static final Fields FIELDS = new Fields(BASE_URL_FIELD, NEW_BASE_URL_FIELD,
                    FETCHED_URL_FIELD, FETCH_TIME_FIELD, CONTENT_FIELD, CONTENT_TYPE_FIELD,
                    RESPONSE_RATE_FIELD, NUM_REDIRECTS_FIELD, HTTP_HEADERS_FIELD);

    public FetchedDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        initFromTupleEntry(new TupleEntry(getStandardFields(), tuple));
    }

    public FetchedDatum(TupleEntry entry, Fields metaDataFields) {
        super(entry.getTuple(), metaDataFields);
        initFromTupleEntry(entry);
    }

    private void initFromTupleEntry(TupleEntry entry) {
        _baseUrl = entry.getString(BASE_URL_FIELD);
        _newBaseUrl = entry.getString(NEW_BASE_URL_FIELD);
        _fetchedUrl = entry.getString(FETCHED_URL_FIELD);
        _fetchTime = entry.getLong(FETCH_TIME_FIELD);
        _content = (BytesWritable) entry.get(CONTENT_FIELD);
        _contentType = entry.getString(CONTENT_TYPE_FIELD);
        _responseRate = entry.getInteger(RESPONSE_RATE_FIELD);
        _numRedirects = entry.getInteger(NUM_REDIRECTS_FIELD);
        _headers = new HttpHeaders(entry.getString(HTTP_HEADERS_FIELD));
    }

    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _baseUrl, _newBaseUrl, _fetchedUrl, _fetchTime, _content,
                        _contentType, _responseRate, _numRedirects, flattenHeaders() };
    }

    private String flattenHeaders() {
        if (_headers == null) {
            return null;
        } else {
            return _headers.toString();
        }
    }

}
