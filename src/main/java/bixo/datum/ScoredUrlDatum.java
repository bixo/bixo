package bixo.datum;

import java.util.Arrays;
import java.util.Map;

import bixo.IConstants;
import bixo.util.FieldUtil;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ScoredUrlDatum extends GroupedUrlDatum implements Comparable<ScoredUrlDatum> {
    private double _score;

    @SuppressWarnings("unchecked")
    public ScoredUrlDatum(String url, long lastFetched, long lastUpdated, FetchStatusCode lastStatus, String groupKey, double score, Map<String, Comparable> metaData) {
        super(url, lastFetched, lastUpdated, lastStatus, groupKey, metaData);
        _score = score;
    }

    public double getScore() {
        return _score;
    }

    public void setScore(double score) {
        _score = score;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getValues() {
        Comparable[] comparables = super.getValues();
        Comparable[] copyOf = Arrays.copyOf(comparables, comparables.length + 1);
        copyOf[comparables.length] = _score;
        return copyOf;
    }

    public static Fields getFields() {
        return FieldUtil.combine(GroupedUrlDatum.getFields(), new Fields(IConstants.SCORE));
    }

    @SuppressWarnings("unchecked")
    public static ScoredUrlDatum fromTuple(Tuple tuple, Fields metaDataFieldNames) {
        TupleEntry entry = new TupleEntry(getFields(), tuple);
        String url = entry.getString(IConstants.URL);
        long lastFetched = entry.getLong(IConstants.LAST_FETCHED);
        long lastUpdated = entry.getLong(IConstants.LAST_UPDATED);
        FetchStatusCode fetchStatus = FetchStatusCode.fromOrdinal(entry.getInteger(IConstants.FETCH_STATUS));
        String groupKey = entry.getString(IConstants.GROUPING_KEY);
        double score = entry.getDouble(IConstants.SCORE);

        Map<String, Comparable> metaData = extractMetaData(tuple, getFields().size(), metaDataFieldNames);
        
        
        return new ScoredUrlDatum(url, lastFetched, lastUpdated, fetchStatus, groupKey, score, metaData);
    }

    @Override
    public int compareTo(ScoredUrlDatum o) {
        // Sort in reverse order, such that higher scores are first.
        if (getScore() > o.getScore()) {
            return -1;
        } else if (getScore() < o.getScore()) {
            return 1;
        } else {
            // TODO KKr - sort by URL, so that if we do a batch fetch, we're
            // fetching pages from the same area of the website.

            // TODO SG adding a simple sting comparison for now.
            return getUrl().compareTo(o.getUrl());
        }
    }
}
