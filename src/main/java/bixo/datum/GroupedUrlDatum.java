package bixo.datum;

import java.util.Arrays;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class GroupedUrlDatum extends UrlDatum {

    private String _groupKey;

    @SuppressWarnings("unchecked")
    public GroupedUrlDatum(String url, long lastFetched, long lastUpdated, UrlStatus lastStatus, String groupKey, Map<String, Comparable> metaData) {
        super(url, lastFetched, lastUpdated, lastStatus, metaData);
        _groupKey = groupKey;
    }

    public String getGroupKey() {
        return _groupKey;
    }

    public void setGroupKey(String groupKey) {
        _groupKey = groupKey;
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String GROUP_KEY_FIELD = fieldName(GroupedUrlDatum.class, "groupKey");
        
    public static final Fields FIELDS = UrlDatum.FIELDS.append(new Fields(GROUP_KEY_FIELD));
    
    public GroupedUrlDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _groupKey = entry.getString(GROUP_KEY_FIELD);
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
        copyOf[baseValues.length] = _groupKey;
        return copyOf;
    }

}
