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
package bixo.examples.crawl;

import com.scaleunlimited.cascading.BaseDatum;

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
        _tupleEntry.setString(URL_FIELD, url);
    }

    public long getLastFetched() {
        return _tupleEntry.getLong(LAST_FETCHED_FIELD);
    }

    public void setLastFetched(long lastFetched) {
        _tupleEntry.setLong(LAST_FETCHED_FIELD, lastFetched);
    }

    public long getLastUpdated() {
        return _tupleEntry.getLong(LAST_UPDATED_FIELD);
    }

    public void setLastUpdated(long lastUpdated) {
        _tupleEntry.setLong(LAST_UPDATED_FIELD, lastUpdated);
    }

    public UrlStatus getLastStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(LAST_STATUS_FIELD));
    }

    public void setLastStatus(UrlStatus lastStatus) {
        _tupleEntry.setString(LAST_STATUS_FIELD, lastStatus.name());
    }

    public int getCrawlDepth() {
        return _tupleEntry.getInteger(CRAWL_DEPTH);
    }

    public void setCrawlDepth(int crawlDepth) {
        _tupleEntry.setInteger(CRAWL_DEPTH, crawlDepth);
    }
    

    public String toString() {
        return getUrl() + "\t" + getLastFetched() + "\t" + getLastUpdated() + "\t" + getLastStatus() + "\t" + getCrawlDepth();
    }
}
