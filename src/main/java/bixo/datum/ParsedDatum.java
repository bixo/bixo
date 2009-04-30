package bixo.datum;

import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ParsedDatum extends BaseDatum {
    private final String _url;
    private final String _parsedText;
    private final Outlink[] _outLinks;

    @SuppressWarnings("unchecked")
    public ParsedDatum(String url, String parsedText, Outlink[] outLinks, Map<String, Comparable> metaData) {
        super(metaData);
        
        _url = url;
        _parsedText = parsedText;
        _outLinks = outLinks;
    }

    public String getUrl() {
        return _url;
    }

    public String getParsedText() {
        return _parsedText;
    }

    public Outlink[] getOulinks() {
        return _outLinks;
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================
    
    // Cascading field names that correspond to the datum fields.
    public static final String URL_FIELD = fieldName(ParsedDatum.class, "url");
    public static final String PARSED_TEXT_FIELD = fieldName(ParsedDatum.class, "parsedText");
    public static final String OUTLINKS_FIELD = fieldName(ParsedDatum.class, "outLinks");

    public static final Fields FIELDS = new Fields(URL_FIELD, PARSED_TEXT_FIELD, OUTLINKS_FIELD);

    public ParsedDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _url = entry.getString(URL_FIELD);
        _parsedText = entry.getString(PARSED_TEXT_FIELD);
        _outLinks = convertFromTuple((Tuple)entry.get(OUTLINKS_FIELD));
    }

    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _url, _parsedText, convertToTuple(_outLinks) };
    }

    private Tuple convertToTuple(Outlink[] outLinks) {
        Tuple tuple = new Tuple();
        for (Outlink outlink : outLinks) {
            tuple.add(outlink.getToUrl());
            tuple.add(outlink.getAnchor());
        }
        return tuple;
    }

    private Outlink[] convertFromTuple(Tuple tuple) {
        int numOutlinks = tuple.size() / 2;
        Outlink[] result = new Outlink[numOutlinks];
        
        for (int i = 0; i < numOutlinks; i++) {
            int tupleOffset = i * 2;
            result[i] = new Outlink(tuple.getString(tupleOffset), tuple.getString(tupleOffset + 1));
        }
        return result;
    }

}
