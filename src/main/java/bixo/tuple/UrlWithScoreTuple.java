package bixo.tuple;

import bixo.IConstants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UrlWithScoreTuple extends UrlWithGroupKeyTuple {
    public static final Fields FIELDS = new Fields(IConstants.SCORE, IConstants.GROUPING_KEY, IConstants.URL, IConstants.LAST_UPDATED, IConstants.LAST_FETCHED, IConstants.LAST_STATUS);

    public UrlWithScoreTuple() {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
    }

    public UrlWithScoreTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

    public double getScore() {
        return getTupleEntry().getDouble(IConstants.SCORE);
    }

    public void SetScore(double score) {
        getTupleEntry().set(IConstants.SCORE, score);
    }
}
