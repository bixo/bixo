package bixo.tuple;

import bixo.IConstants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UrlWithGroupKeyTuple extends UrlTuple {

    public static final Fields FIELDS = new Fields(IConstants.GROUPING_KEY, IConstants.URL, IConstants.LAST_UPDATED, IConstants.LAST_FETCHED, IConstants.LAST_STATUS);

    public UrlWithGroupKeyTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

    public UrlWithGroupKeyTuple(TupleEntry tupleEntry) {
        super(tupleEntry);
    }

    public String getGroupingKet() {
        return getTupleEntry().getString(IConstants.GROUPING_KEY);
    }

}
