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
    private String _fetchedUrl;
    private long _fetchTime;
    private BytesWritable _content;
    private String _contentType;
    private int _responseRate;
    private HttpHeaders _headers;
    
    @SuppressWarnings("unchecked")
    public FetchedDatum(String baseUrl, String redirectedUrl, long fetchTime, HttpHeaders headers, BytesWritable content, String contentType, int responseRate, Map<String, Comparable> metaData) {
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
        _fetchedUrl = redirectedUrl;
        _fetchTime = fetchTime;
        _content = content;
        _contentType = contentType;
        _responseRate = responseRate;
        _headers = headers;
    }

    
    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param url - Base & redirected url
     * @param metaData - metadata
     */
    @SuppressWarnings("unchecked")
	public FetchedDatum(String url, Map<String, Comparable> metaData) {
        this(url, url, 0, new HttpHeaders(), new BytesWritable(), "", 0, metaData);
    }
    
    
    /**
     * Create place-holder FetchedDatum from the data used to attempt the fetch.
     * 
     * @param scoredDatum Valid datum with url/metadata needed to create FetchedDatum
     */
    public FetchedDatum(final ScoredUrlDatum scoredDatum) {
        this(scoredDatum.getUrl(), scoredDatum.getMetaDataMap());
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

    public HttpHeaders getHeaders() {
        return _headers;
    }

    @Override
	public String toString() {
        StringBuilder result = new StringBuilder("[base URL] ");
        result.append(_baseUrl);
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
    public static final String FETCHED_URL_FIELD = fieldName(FetchedDatum.class, "fetchedUrl");
    public static final String FETCH_TIME_FIELD = fieldName(FetchedDatum.class, "fetchTime");
    public static final String CONTENT_FIELD = fieldName(FetchedDatum.class, "content");
    public static final String CONTENT_TYPE_FIELD = fieldName(FetchedDatum.class, "contentType");
    public static final String RESPONSE_RATE_FIELD = fieldName(FetchedDatum.class, "responseRate");
    public static final String HTTP_HEADERS_FIELD = fieldName(FetchedDatum.class, "httpHeaders");

    public static final Fields FIELDS = new Fields(BASE_URL_FIELD, FETCHED_URL_FIELD, FETCH_TIME_FIELD, CONTENT_FIELD, CONTENT_TYPE_FIELD, RESPONSE_RATE_FIELD, HTTP_HEADERS_FIELD);

    /**
     * Create FetchedDatum using a line that we read in from a text file generated by writing
     * out a FetchedDatum using a Cascading TextLine scheme.
     * 
     * Note that we can't handle metadata fields, as these have been converted to strings, so
     * we don't know what Comparable type to use when converting back to values in the tuple.
     * 
     * @param textLine
     * @return new FetchedDatum
     */
    public static FetchedDatum createFromTextLine(String textLine) {
        Tuple tuple = new Tuple();
        
        String fields[] = textLine.split("\t");
        if (fields.length < FIELDS.size()) {
            throw new InvalidParameterException("Not enough fields in textLine");
        }
        
        tuple.add(fields[0]);                           // baseUrl
        tuple.add(fields[1]);                           // fetchedUrl
        tuple.add(Long.parseLong(fields[2]));           // fetchTime
        tuple.add(stringToBytesWritable(fields[3]));    // content
        tuple.add(fields[4]);                           // contentType
        tuple.add(Integer.parseInt(fields[5]));         // responseRate
        tuple.add(fields[6]);                           // headers
        
        return new FetchedDatum(tuple, new Fields());
    }
    
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
        _fetchedUrl = entry.getString(FETCHED_URL_FIELD);
        _fetchTime = entry.getLong(FETCH_TIME_FIELD);
        _content = (BytesWritable)entry.get(CONTENT_FIELD);
        _contentType = entry.getString(CONTENT_TYPE_FIELD);
        _responseRate = entry.getInteger(RESPONSE_RATE_FIELD);
        _headers = new HttpHeaders(entry.getString(HTTP_HEADERS_FIELD));
    }
    
    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _baseUrl, _fetchedUrl, _fetchTime, _content, _contentType, _responseRate, flattenHeaders() };
    }

    private String flattenHeaders() {
        if (_headers == null) {
            return null;
        } else {
            return _headers.toString();
        }
    }
    
    private static BytesWritable stringToBytesWritable(final String hexData) {
        // Figure out how many bytes we have. Format will be "HH HH HH ", though there
        // may or may not be a trailing space.
        char[] chars = hexData.toCharArray();
        
        int numBytes = (chars.length + 2) / 3;
        byte[] bytes = new byte[numBytes];
        
        int charOffset = 0;
        for (int i = 0; i < numBytes; i++, charOffset += 3) {
            bytes[i] = (byte)((Character.digit(chars[charOffset], 16) * 16) + Character.digit(chars[charOffset + 1], 16));
        }
        
        return new BytesWritable(bytes);
    }
}
