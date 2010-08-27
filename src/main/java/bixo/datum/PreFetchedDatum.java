package bixo.datum;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class PreFetchedDatum extends BaseDatum {
    private List<ScoredUrlDatum> _urls;
    private long _fetchTime;
    private long _fetchDelay;
    private int _groupingKey;
    private String _groupingRef;
    private boolean _lastList;
    private boolean _skipped;
    
    public PreFetchedDatum(List<ScoredUrlDatum> urls, long fetchTime, long fetchDelay, int groupingKey, String groupingRef, boolean lastList) {
        super(BaseDatum.EMPTY_METADATA_MAP);
        
        _urls = urls;
        _fetchTime = fetchTime;
        _fetchDelay = fetchDelay;
        _groupingKey = groupingKey;
        _groupingRef = groupingRef;
        _lastList = lastList;
        _skipped = false;
    }

    public List<ScoredUrlDatum> getUrls() {
        return _urls;
    }
    
    public void setUrls(List<ScoredUrlDatum> urls) {
        _urls = urls;
    }
    
    public long getFetchTime() {
        return _fetchTime;
    }
    
    public void setFetchTime(long fetchTime) {
        _fetchTime = fetchTime;
    }
    
    public long getFetchDelay() {
        return _fetchDelay;
    }

    public void setFetchDelay(long fetchDelay) {
        _fetchDelay = fetchDelay;
    }

    public int getGroupingKey() {
        return _groupingKey;
    }

    public void setGroupingKey(int groupingKey) {
        _groupingKey = groupingKey;
    }

    public String getGroupingRef() {
        return _groupingRef;
    }
    
    public void setGroupignRef(String groupingRef) {
        _groupingRef = groupingRef;
    }
    
    public boolean isLastList() {
        return _lastList;
    }

    public void setLastList(boolean lastList) {
        _lastList = lastList;
    }

    public boolean isSkipped() {
        return _skipped;
    }
    
    public void setSkipped(boolean skipped) {
        _skipped = skipped;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (_fetchDelay ^ (_fetchDelay >>> 32));
        result = prime * result + (int) (_fetchTime ^ (_fetchTime >>> 32));
        result = prime * result + _groupingKey;
        result = prime * result + ((_groupingRef == null) ? 0 : _groupingRef.hashCode());
        result = prime * result + (_lastList ? 1231 : 1237);
        result = prime * result + (_skipped ? 1231 : 1237);
        result = prime * result + ((_urls == null) ? 0 : _urls.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        PreFetchedDatum other = (PreFetchedDatum) obj;
        if (_fetchDelay != other._fetchDelay)
            return false;
        if (_fetchTime != other._fetchTime)
            return false;
        if (_groupingKey != other._groupingKey)
            return false;
        if (_groupingRef == null) {
            if (other._groupingRef != null)
                return false;
        } else if (!_groupingRef.equals(other._groupingRef))
            return false;
        if (_lastList != other._lastList)
            return false;
        if (_skipped != other._skipped)
            return false;
        if (_urls == null) {
            if (other._urls != null)
                return false;
        } else if (!_urls.equals(other._urls))
            return false;
        return true;
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================

    // Cascading field names that correspond to the datum fields.
    public static final String URLS_FN = fieldName(PreFetchedDatum.class, "urls");
    public static final String FETCH_TIME_FN = fieldName(PreFetchedDatum.class, "fetchTime");
    public static final String FETCH_DELAY_FN = fieldName(PreFetchedDatum.class, "fetchDelay");
    public static final String GROUPING_KEY_FN = fieldName(PreFetchedDatum.class, "groupingKey");
    public static final String GROUPING_REF_FN = fieldName(PreFetchedDatum.class, "groupingRef");
    public static final String LAST_LIST_FN = fieldName(PreFetchedDatum.class, "lastList");
    public static final String SKIPPED_FN = fieldName(PreFetchedDatum.class, "skipped");
    
    public static final Fields FIELDS = new Fields(URLS_FN, FETCH_TIME_FN, FETCH_DELAY_FN, GROUPING_KEY_FN, GROUPING_REF_FN, LAST_LIST_FN, SKIPPED_FN);
    
    public PreFetchedDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, BaseDatum.EMPTY_METADATA_FIELDS);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _urls = tupleToList((Tuple)entry.get(URLS_FN), metaDataFields);
        _fetchTime = entry.getLong(FETCH_TIME_FN);
        _fetchDelay = entry.getLong(FETCH_DELAY_FN);
        _groupingKey = entry.getInteger(GROUPING_KEY_FN);
        _groupingRef = entry.getString(GROUPING_REF_FN);
        _lastList = entry.getBoolean(LAST_LIST_FN);
        _skipped = entry.getBoolean(SKIPPED_FN);
    }
    
    
    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return BaseDatum.makeStandardValues(listToTuple(_urls), _fetchTime, _fetchDelay, _groupingKey, _groupingRef, _lastList, _skipped);
    }

    @SuppressWarnings("unchecked")
    private static List<ScoredUrlDatum> tupleToList(Tuple tuple, Fields metaDataFields) {
        List<ScoredUrlDatum> result = new LinkedList<ScoredUrlDatum>();
        Iterator iter = tuple.iterator();
        while (iter.hasNext()) {
            result.add(new ScoredUrlDatum((Tuple)iter.next(), metaDataFields));
        }
        
        return result;
    }
    
    private static Tuple listToTuple(List<ScoredUrlDatum> list) {
        Tuple result = new Tuple();
        for (ScoredUrlDatum datum : list) {
            result.add(datum.toTuple());
        }
        
        return result;
    }
}
