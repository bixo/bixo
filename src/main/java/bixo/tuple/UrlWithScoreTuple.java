package bixo.tuple;

import bixo.Constants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UrlWithScoreTuple extends UrlWithGroupKeyTuple {
    public static final Fields FIELDS = new Fields(Constants.SCORE, Constants.GROUPING_KEY, Constants.URL, Constants.LAST_UPDATED, Constants.LAST_FETCHED, Constants.LAST_STATUS);

    public UrlWithScoreTuple() {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
    }

    public UrlWithScoreTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

    public double getScore() {
        return getTupleEntry().getDouble(Constants.SCORE);
    }

    public void SetScore(double score) {
        getTupleEntry().set(Constants.SCORE, score);
    }
}
