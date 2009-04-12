package bixo.tuple;

import bixo.Constants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UrlWithGroupKeyTuple extends UrlTuple {

    public static final Fields FIELDS = new Fields(Constants.GROUPING_KEY, Constants.URL, Constants.LAST_UPDATED, Constants.LAST_FETCHED, Constants.LAST_STATUS);

    public UrlWithGroupKeyTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

    public UrlWithGroupKeyTuple(TupleEntry tupleEntry) {
        super(tupleEntry);
    }

    public String getGroupingKet() {
        return getTupleEntry().getString(Constants.GROUPING_KEY);
    }

}
