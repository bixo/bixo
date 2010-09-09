package bixo.datum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import bixo.cascading.Datum;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * A FetchSetDatum represents a group of URLs that will be fetched using one
 * persistent connection to the target server.
 *
 */
@SuppressWarnings("serial")
public class FetchSetDatum extends Datum {
    
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

    public FetchSetDatum(List<ScoredUrlDatum> urls, long fetchTime, long fetchDelay, int groupingKey, String groupingRef, boolean lastList) {
        super(FIELDS);
        
        setUrls(urls);
        setFetchTime(fetchTime);
        setFetchDelay(fetchDelay);
        setGroupingKey(groupingKey);
        setGroupingRef(groupingRef);
        setLastList(lastList);
        setSkipped(false);
    }

    @SuppressWarnings("unchecked")
    public List<ScoredUrlDatum> getUrls() {
        Tuple urls = (Tuple)_tupleEntry.get(URLS_FN);
        List<ScoredUrlDatum> result = new ArrayList<ScoredUrlDatum>(urls.size());
        Iterator<Tuple> iter = (Iterator<Tuple>)urls.iterator();
        while (iter.hasNext()) {
            result.add(new ScoredUrlDatum(iter.next()));
        }
        
        return result;
    }
    
    public void setUrls(List<ScoredUrlDatum> urls) {
        Tuple result = new Tuple();
        for (ScoredUrlDatum datum : urls) {
            result.add(datum.getTuple());
        }
        
        _tupleEntry.set(URLS_FN, result);
    }
    
    public long getFetchTime() {
        return _tupleEntry.getLong(FETCH_TIME_FN);
    }
    
    public void setFetchTime(long fetchTime) {
        _tupleEntry.set(FETCH_TIME_FN, fetchTime);
    }
    
    public long getFetchDelay() {
        return _tupleEntry.getLong(FETCH_DELAY_FN);
    }

    public void setFetchDelay(long fetchDelay) {
        _tupleEntry.set(FETCH_DELAY_FN, fetchDelay);
    }

    public int getGroupingKey() {
        return _tupleEntry.getInteger(GROUPING_KEY_FN);
    }

    public void setGroupingKey(int groupingKey) {
        _tupleEntry.set(GROUPING_KEY_FN, groupingKey);
    }

    public String getGroupingRef() {
        return _tupleEntry.getString(GROUPING_REF_FN);
    }
    
    public void setGroupingRef(String groupingRef) {
        _tupleEntry.set(GROUPING_REF_FN, groupingRef);
    }
    
    public boolean isLastList() {
        return _tupleEntry.getBoolean(LAST_LIST_FN);
    }

    public void setLastList(boolean lastList) {
        _tupleEntry.set(LAST_LIST_FN, lastList);
    }

    public boolean isSkipped() {
        return _tupleEntry.getBoolean(SKIPPED_FN);
    }
    
    public void setSkipped(boolean skipped) {
        _tupleEntry.set(SKIPPED_FN, skipped);
    }

    // ==================================================
    
    public static Fields getGroupingField() {
        return new Fields(GROUPING_KEY_FN);
    }

    public static Fields getSortingField() {
        return new Fields(FETCH_TIME_FN);
    }
}
