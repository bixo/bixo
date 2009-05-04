package bixo.datum;

import java.util.Arrays;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ScoredUrlDatum extends GroupedUrlDatum implements Comparable<ScoredUrlDatum> {
    private double _score;

    @SuppressWarnings("unchecked")
    public ScoredUrlDatum(String url, long lastFetched, long lastUpdated, FetchStatusCode lastStatus, String normalizedUrl, String groupKey, double score, Map<String, Comparable> metaData) {
        super(url, lastFetched, lastUpdated, lastStatus, normalizedUrl, groupKey, metaData);
        _score = score;
    }

    public double getScore() {
        return _score;
    }

    public void setScore(double score) {
        _score = score;
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
            return getNormalizedUrl().compareTo(o.getNormalizedUrl());
        }
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String SCORE_FIELD = fieldName(ScoredUrlDatum.class, "score");
        
    public static final Fields FIELDS = GroupedUrlDatum.FIELDS.append(new Fields(SCORE_FIELD));
    
    public ScoredUrlDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _score = entry.getDouble(SCORE_FIELD);
    };
    
    
    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        Comparable[] baseValues = super.getStandardValues();
        Comparable[] copyOf = Arrays.copyOf(baseValues, baseValues.length + 1);
        copyOf[baseValues.length] = _score;
        return copyOf;
    }

}
