package bixo.datum;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ParsedDatum extends BaseDatum {
    private String _url;
    private String _parsedText;
    private String _language;
    private String _title;
    private Outlink[] _outlinks;
    private Map<String, String> _parsedMeta;
    
    /**
     * No argument constructor for use with FutureTask
     */
    public ParsedDatum() {
        super(BaseDatum.EMPTY_METADATA_MAP);
    }
    
    @SuppressWarnings("unchecked")
    public ParsedDatum(String url, String parsedText, String language, String title, Outlink[] outlinks, Map<String, String> parsedMeta, Map<String, Comparable> metaData) {
        super(metaData);
        
        _url = url;
        _parsedText = parsedText;
        _language = language;
        _title = title;
        _outlinks = outlinks;
        _parsedMeta = parsedMeta;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public String getParsedText() {
        return _parsedText;
    }

    public void setParsedText(String parsedText) {
        _parsedText = parsedText;
    }

    public String getLanguage() {
        return _language;
    }

    public void setLanguage(String language) {
        _language = language;
    }

    public String getTitle() {
        return _title;
    }

    public void setTitle(String title) {
        _title = title;
    }

    public Outlink[] getOutlinks() {
        return _outlinks;
    }

    public void setOutlinks(Outlink[] outlinks) {
        _outlinks = outlinks;
    }

    public Map<String, String> getParsedMeta() {
        return _parsedMeta;
    }

    public void setParsedMeta(Map<String, String> parsedMeta) {
        _parsedMeta = parsedMeta;
    }

    // ======================================================================================
    // Below here is all Cascading-specific implementation
    // ======================================================================================

    // Cascading field names that correspond to the datum fields.
    public static final String URL_FIELD = fieldName(ParsedDatum.class, "url");
    public static final String PARSED_TEXT_FIELD = fieldName(ParsedDatum.class, "parsedText");
    public static final String LANGUAGE_FIELD = fieldName(ParsedDatum.class, "language");
    public static final String TITLE_FIELD = fieldName(ParsedDatum.class, "title");
    public static final String OUTLINKS_FIELD = fieldName(ParsedDatum.class, "outLinks");
    public static final String PARSED_META_FIELD = fieldName(ParsedDatum.class, "parsedMeta");

    public static final Fields FIELDS = new Fields(URL_FIELD, PARSED_TEXT_FIELD, LANGUAGE_FIELD, TITLE_FIELD, OUTLINKS_FIELD, PARSED_META_FIELD);

    public ParsedDatum(Tuple tuple, Fields metaDataFields) {
        super(tuple, metaDataFields);
        
        TupleEntry entry = new TupleEntry(getStandardFields(), tuple);
        _url = entry.getString(URL_FIELD);
        _parsedText = entry.getString(PARSED_TEXT_FIELD);
        _language = entry.getString(LANGUAGE_FIELD);
        _title = entry.getString(TITLE_FIELD);
        _outlinks = convertTupleToOutlinks((Tuple)entry.get(OUTLINKS_FIELD));
        _parsedMeta = convertTupleToMap((Tuple)entry.get(PARSED_META_FIELD));
    }

    @Override
    public Fields getStandardFields() {
        return FIELDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getStandardValues() {
        return new Comparable[] { _url, _parsedText, _language, _title, convertOutlinksToTuple(_outlinks), convertMapToTuple(_parsedMeta) };
    }

    private Tuple convertOutlinksToTuple(Outlink[] outLinks) {
        Tuple tuple = new Tuple();
        for (Outlink outlink : outLinks) {
            tuple.add(outlink.getToUrl());
            tuple.add(outlink.getAnchor());
        }
        return tuple;
    }

    private Outlink[] convertTupleToOutlinks(Tuple tuple) {
        int numOutlinks = tuple.size() / 2;
        Outlink[] result = new Outlink[numOutlinks];
        
        for (int i = 0; i < numOutlinks; i++) {
            int tupleOffset = i * 2;
            result[i] = new Outlink(tuple.getString(tupleOffset), tuple.getString(tupleOffset + 1));
        }
        return result;
    }
    
    private Tuple convertMapToTuple(Map<String, String> map) {
        Tuple result = new Tuple();
        if (map != null) {
            for (Entry<String, String> entry : map.entrySet()) {
                result.add(entry.getKey());
                result.add(entry.getValue());
            }
        }
        
        return result;
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, String> convertTupleToMap(Tuple tuple) {
        Map<String, String> result = new HashMap<String, String>();
        Iterator<Comparable> iter = tuple.iterator();
        while (iter.hasNext()) {
            String key = (String)iter.next();
            String value = (String)iter.next();
            result.put(key, value);
        }
        return result;
    }

}
