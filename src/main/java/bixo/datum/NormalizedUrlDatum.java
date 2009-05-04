package bixo.datum;

import java.util.Arrays;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NormalizedUrlDatum extends UrlDatum {

    private String _normalizedUrl;

    @SuppressWarnings("unchecked")
    public NormalizedUrlDatum(String url, long lastFetched, long lastUpdated, FetchStatusCode lastStatus, String normalizedUrl, Map<String, Comparable> metaData) {
        super(url, lastFetched, lastUpdated, lastStatus, metaData);
        _normalizedUrl = normalizedUrl;
    }

    public String getNormalizedUrl() {
        return _normalizedUrl;
    }

    public void setNormalizedUrl(String normalizedUrl) {
        _normalizedUrl = normalizedUrl;
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String NORMALIZED_URL_FIELD = fieldName(NormalizedUrlDatum.class, "normalizedUrl");
        
    public static final Fields FIELDS = UrlDatum.FIELDS.append(new Fields(NORMALIZED_URL_FIELD));
    
    public NormalizedUrlDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _normalizedUrl = entry.getString(NORMALIZED_URL_FIELD);
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
        copyOf[baseValues.length] = _normalizedUrl;
        return copyOf;
    }

}
