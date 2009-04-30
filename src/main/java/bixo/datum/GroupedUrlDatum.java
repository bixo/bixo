package bixo.datum;

import java.util.Arrays;
import java.util.Map;

import bixo.utils.FieldUtil;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class GroupedUrlDatum extends UrlDatum {

    private String _groupKey;

    @SuppressWarnings("unchecked")
    public GroupedUrlDatum(String url, long lastFetched, long lastUpdated, FetchStatusCode lastStatus, String groupKey, Map<String, Comparable> metaData) {
        super(url, lastFetched, lastUpdated, lastStatus, metaData);
        _groupKey = groupKey;
    }

    public String getGroupKey() {
        return _groupKey;
    }

    public void setGroupKey(String groupKey) {
        _groupKey = groupKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getValues() {
        Comparable[] comparables = super.getValues();
        Comparable[] copyOf = Arrays.copyOf(comparables, comparables.length + 1);
        copyOf[comparables.length] = _groupKey;
        return copyOf;
    }

    public static Fields getFields() {
        return FieldUtil.combine(UrlDatum.getFields(), new Fields(IFieldNames.GROUPING_KEY));
    }

    public static GroupedUrlDatum fromTuple(Tuple tuple, Fields metaDataFieldNames) {
        TupleEntry entry = new TupleEntry(getFields(), tuple);
        String url = entry.getString(IFieldNames.SOURCE_URL);
        long lastFetched = entry.getLong(IFieldNames.SOURCE_LAST_FETCHED);
        long lastUpdated = entry.getLong(IFieldNames.SOURCE_LAST_UPDATED);
        FetchStatusCode fetchStatus = FetchStatusCode.fromOrdinal(entry.getInteger(IFieldNames.SOURCE_FETCH_STATUS));
        String groupKey = entry.getString(IFieldNames.GROUPING_KEY);

        return new GroupedUrlDatum(url, lastFetched, lastUpdated, fetchStatus, groupKey, BaseDatum.extractMetaData(tuple, getFields().size(), metaDataFieldNames));
    }

}
