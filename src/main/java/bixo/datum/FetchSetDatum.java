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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.scaleunlimited.cascading.BaseDatum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/**
 * A FetchSetDatum represents a group of URLs that will be fetched using one
 * persistent connection to the target server.
 *
 */
@SuppressWarnings("serial")
public class FetchSetDatum extends BaseDatum {
    
    private static final String URLS_FN = fieldName(FetchSetDatum.class, "urls");
    private static final String FETCH_TIME_FN = fieldName(FetchSetDatum.class, "fetchTime");
    private static final String FETCH_DELAY_FN = fieldName(FetchSetDatum.class, "fetchDelay");
    private static final String GROUPING_KEY_FN = fieldName(FetchSetDatum.class, "groupingKey");
    private static final String GROUPING_REF_FN = fieldName(FetchSetDatum.class, "groupingRef");
    private static final String LAST_LIST_FN = fieldName(FetchSetDatum.class, "lastList");
    private static final String SKIPPED_FN = fieldName(FetchSetDatum.class, "skipped");
    
    public static final Fields FIELDS = new Fields(URLS_FN, FETCH_TIME_FN, FETCH_DELAY_FN, GROUPING_KEY_FN, GROUPING_REF_FN, LAST_LIST_FN, SKIPPED_FN);

    public FetchSetDatum() {
        super(FIELDS);
    }
    
    public FetchSetDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }
    
    public FetchSetDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }

    public FetchSetDatum(List<ScoredUrlDatum> urls, long fetchTime, long fetchDelay, int groupingKey, String groupingRef) {
        super(FIELDS);
        
        setUrls(urls);
        setFetchTime(fetchTime);
        setFetchDelay(fetchDelay);
        setGroupingKey(groupingKey);
        setGroupingRef(groupingRef);
        setLastList(false);
        setSkipped(false);
    }

    public List<ScoredUrlDatum> getUrls() {
        Tuple urls = (Tuple)_tupleEntry.getObject(URLS_FN);
        List<ScoredUrlDatum> result = new ArrayList<ScoredUrlDatum>(urls.size());
        Iterator<Object> iter = urls.iterator();
        while (iter.hasNext()) {
            result.add(new ScoredUrlDatum((Tuple)iter.next()));
        }
        
        return result;
    }
    
    public void setUrls(List<ScoredUrlDatum> urls) {
        Tuple result = new Tuple();
        for (ScoredUrlDatum datum : urls) {
            result.add(datum.getTuple());
        }
        
        _tupleEntry.setObject(URLS_FN, result);
    }
    
    public long getFetchTime() {
        return _tupleEntry.getLong(FETCH_TIME_FN);
    }
    
    public void setFetchTime(long fetchTime) {
        _tupleEntry.setLong(FETCH_TIME_FN, fetchTime);
    }
    
    public long getFetchDelay() {
        return _tupleEntry.getLong(FETCH_DELAY_FN);
    }

    public void setFetchDelay(long fetchDelay) {
        _tupleEntry.setLong(FETCH_DELAY_FN, fetchDelay);
    }

    public int getGroupingKey() {
        return _tupleEntry.getInteger(GROUPING_KEY_FN);
    }

    public void setGroupingKey(int groupingKey) {
        _tupleEntry.setInteger(GROUPING_KEY_FN, groupingKey);
    }

    public String getGroupingRef() {
        return _tupleEntry.getString(GROUPING_REF_FN);
    }
    
    public void setGroupingRef(String groupingRef) {
        _tupleEntry.setString(GROUPING_REF_FN, groupingRef);
    }
    
    public boolean isLastList() {
        return _tupleEntry.getBoolean(LAST_LIST_FN);
    }

    public void setLastList(boolean lastList) {
        _tupleEntry.setBoolean(LAST_LIST_FN, lastList);
    }

    public boolean isSkipped() {
        return _tupleEntry.getBoolean(SKIPPED_FN);
    }
    
    public void setSkipped(boolean skipped) {
        _tupleEntry.setBoolean(SKIPPED_FN, skipped);
    }

    // ==================================================
    
    public static Fields getGroupingField() {
        return new Fields(GROUPING_KEY_FN);
    }

    public static Fields getSortingField() {
        return new Fields(FETCH_TIME_FN);
    }

}
