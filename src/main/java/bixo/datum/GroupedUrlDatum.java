package bixo.datum;

import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class GroupedUrlDatum extends UrlDatum implements Serializable {

    private static final String GROUP_KEY_FN = fieldName(GroupedUrlDatum.class, "groupKey");
    public static final Fields FIELDS = new Fields(GROUP_KEY_FN).append(getSuperFields(GroupedUrlDatum.class));
    
    public GroupedUrlDatum() {
        super(FIELDS);
    }
    
    public GroupedUrlDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }
    
    public GroupedUrlDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
        validateFields(fields, FIELDS);
    }

    public GroupedUrlDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }

    public GroupedUrlDatum(String url, String groupKey) {
        super(FIELDS, url);
        setGroupKey(groupKey);
    }

    public GroupedUrlDatum(Fields fields, String url, String groupKey) {
        super(fields, url);
        setGroupKey(groupKey);
    }

    public GroupedUrlDatum(UrlDatum datum, String groupKey) {
        super(FIELDS, datum.getUrl());
        setGroupKey(groupKey);
        setPayload(datum);
    }
    
    public String getGroupKey() {
        return _tupleEntry.getString(GROUP_KEY_FN);
    }

    public void setGroupKey(String groupKey) {
        _tupleEntry.set(GROUP_KEY_FN, groupKey);
    }
    
    public static Fields getGroupingField() {
        return new Fields(GROUP_KEY_FN);
    }
}
