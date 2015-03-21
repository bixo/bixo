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
package bixo.examples.webmining;

import com.scaleunlimited.cascading.BaseDatum;

import bixo.datum.UrlStatus;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


@SuppressWarnings("serial")
public class CrawlDbDatum extends BaseDatum {
    
    public static final String URL_FIELD = fieldName(CrawlDbDatum.class, "url");
    public static final String LAST_FETCHED_FIELD = fieldName(CrawlDbDatum.class, "lastFetched");
    public static final String LAST_STATUS_FIELD = fieldName(CrawlDbDatum.class, "lastStatus");
    public static final String LINKS_SCORE_FIELD = fieldName(CrawlDbDatum.class, "linksScore");
    public static final String PAGE_SCORE_FIELD = fieldName(CrawlDbDatum.class, "pageScore");
    
    
    public static final Fields FIELDS = new Fields(URL_FIELD,
                    LAST_FETCHED_FIELD, LAST_STATUS_FIELD,
                    LINKS_SCORE_FIELD, PAGE_SCORE_FIELD);

    @SuppressWarnings("rawtypes")
    public static Class[] TYPES = {String.class, 
                            Long.class, String.class,
                            Float.class, Float.class};
    
    public CrawlDbDatum() {
        super(FIELDS);
    }

    public CrawlDbDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public CrawlDbDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    public CrawlDbDatum(String url) {
        this(url, 0, UrlStatus.UNFETCHED, 0.0f, 0.0f);
    }
    
    public CrawlDbDatum(String url, long lastFetched, UrlStatus lastStatus, float linksScore, float pageScore) {
        super(FIELDS);
        
        setUrl(url);
        setLastFetched(lastFetched);
        setLastStatus(lastStatus);
        setLinksScore(linksScore);
        setPageScore(pageScore);
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

    public UrlStatus getLastStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(LAST_STATUS_FIELD));
    }

    public void setLastStatus(UrlStatus lastStatus) {
        _tupleEntry.setString(LAST_STATUS_FIELD, lastStatus.name());
    }

    public float getLinksScore() {
        return _tupleEntry.getFloat(LINKS_SCORE_FIELD);
    }

    public void setLinksScore(float linksScore) {
        _tupleEntry.setFloat(LINKS_SCORE_FIELD, linksScore);
    }
    
    public float getPageScore() {
        return _tupleEntry.getFloat(PAGE_SCORE_FIELD);
    }

    public void setPageScore(float pageScore) {
        _tupleEntry.setFloat(PAGE_SCORE_FIELD, pageScore);
    }
    
    public String toString() {
        return String.format("Page %s: last status %s, links score %.4f, page score %.4f",
                        getUrl(),
                        getLastStatus(),
                        getLinksScore(),
                        getPageScore());
    }
}
