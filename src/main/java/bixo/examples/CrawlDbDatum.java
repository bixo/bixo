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
package bixo.examples;

import bixo.cascading.BaseDatum;
import bixo.datum.UrlStatus;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class CrawlDbDatum extends BaseDatum {
    

    public static final String URL_FIELD = fieldName(CrawlDbDatum.class, "url");
    public static final String LAST_FETCHED_FIELD = fieldName(CrawlDbDatum.class, "lastFetched");
    public static final String LAST_UPDATED_FIELD = fieldName(CrawlDbDatum.class, "lastUpdated");
    public static final String LAST_STATUS_FIELD = fieldName(CrawlDbDatum.class, "lastStatus");
    public static final String CRAWL_DEPTH = fieldName(CrawlDbDatum.class, "crawlDepth");
    
    
    public static final Fields FIELDS = new Fields(URL_FIELD, LAST_FETCHED_FIELD, LAST_UPDATED_FIELD, LAST_STATUS_FIELD, CRAWL_DEPTH);

    public CrawlDbDatum () {
        super(FIELDS);
    }

    public CrawlDbDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    public CrawlDbDatum(String url, long lastFetched, long lastUpdated, UrlStatus lastStatus, int crawlDepth) {
        super(FIELDS);
        setUrl(url);
        setLastFetched(lastFetched);
        setLastUpdated(lastUpdated);
        setLastStatus(lastStatus);
        setCrawlDepth(crawlDepth);
       }

    public String getUrl() {
        return _tupleEntry.getString(URL_FIELD);
    }

    public void setUrl(String url) {
        _tupleEntry.set(URL_FIELD, url);
    }

    public long getLastFetched() {
        return _tupleEntry.getLong(LAST_FETCHED_FIELD);
    }

    public void setLastFetched(long lastFetched) {
        _tupleEntry.set(LAST_FETCHED_FIELD, lastFetched);
    }

    public long getLastUpdated() {
        return _tupleEntry.getLong(LAST_UPDATED_FIELD);
    }

    public void setLastUpdated(long lastUpdated) {
        _tupleEntry.set(LAST_UPDATED_FIELD, lastUpdated);
    }

    public UrlStatus getLastStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(LAST_STATUS_FIELD));
    }

    public void setLastStatus(UrlStatus lastStatus) {
        _tupleEntry.set(LAST_STATUS_FIELD, lastStatus.name());
    }

    public int getCrawlDepth() {
        return _tupleEntry.getInteger(CRAWL_DEPTH);
    }

    public void setCrawlDepth(int crawlDepth) {
        _tupleEntry.set(CRAWL_DEPTH, crawlDepth);
    }
    

    public String toString() {
        return getUrl() + "\t" + getLastFetched() + "\t" + getLastUpdated() + "\t" + getLastStatus() + "\t" + getCrawlDepth();
    }
}
