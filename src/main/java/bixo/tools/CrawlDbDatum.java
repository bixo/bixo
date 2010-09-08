/*
 * Copyright (c) 2010 Bixo Labs.
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
package bixo.tools;

import bixo.datum.BaseDatum;
import bixo.datum.UrlStatus;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class CrawlDbDatum extends BaseDatum {
    private String _url;
    private long _lastFetched;
    private long _lastUpdated;
    private UrlStatus _lastStatus;
    private int _crawlDepth;
    
    public CrawlDbDatum(String url, long lastFetched, long lastUpdated, UrlStatus lastStatus, int crawlDepth) {
        super(BaseDatum.EMPTY_METADATA_MAP);
        
        _url = url;
        _lastFetched = lastFetched;
        _lastUpdated = lastUpdated;
        _lastStatus = lastStatus;
        _crawlDepth = crawlDepth;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public long getLastFetched() {
        return _lastFetched;
    }

    public void setLastFetched(long lastFetched) {
        _lastFetched = lastFetched;
    }

    public long getLastUpdated() {
        return _lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        _lastUpdated = lastUpdated;
    }

    public UrlStatus getLastStatus() {
        return _lastStatus;
    }

    public void setLastStatus(UrlStatus lastStatus) {
        _lastStatus = lastStatus;
    }

    public int getCrawlDepth() {
        return _crawlDepth;
    }

    public void setLastStatus(int crawlDepth) {
        _crawlDepth = crawlDepth;
    }
    
    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String URL_FIELD = fieldName(CrawlDbDatum.class, "url");
    public static final String LAST_FETCHED_FIELD = fieldName(CrawlDbDatum.class, "lastFetched");
    public static final String LAST_UPDATED_FIELD = fieldName(CrawlDbDatum.class, "lastUpdated");
    public static final String LAST_STATUS_FIELD = fieldName(CrawlDbDatum.class, "lastStatus");
    public static final String CRAWL_DEPTH = fieldName(CrawlDbDatum.class, "crawlDepth");
    
    
    public static final Fields FIELDS = new Fields(URL_FIELD, LAST_FETCHED_FIELD, LAST_UPDATED_FIELD, LAST_STATUS_FIELD, CRAWL_DEPTH);
    public static final Fields PAYLOAD_FIELDS = new Fields(LAST_FETCHED_FIELD, LAST_UPDATED_FIELD, LAST_STATUS_FIELD, CRAWL_DEPTH);

    public CrawlDbDatum(Tuple tuple) {
        super(tuple, BaseDatum.EMPTY_METADATA_FIELDS);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _url = entry.getString(URL_FIELD);
        _lastFetched = entry.getLong(LAST_FETCHED_FIELD);
        _lastUpdated = entry.getLong(LAST_UPDATED_FIELD);
        _lastStatus = UrlStatus.valueOf(entry.getString(LAST_STATUS_FIELD));
        _crawlDepth = entry.getInteger(CRAWL_DEPTH);;
   }
    
    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _url, _lastFetched, _lastUpdated, _lastStatus.name(), _crawlDepth };
    }

    public String toString() {
        return _url + "\t" + _lastFetched + "\t" + _lastUpdated + "\t" + _lastStatus + "\t" + _crawlDepth;
    }
}
