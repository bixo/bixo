package bixo.datum;

import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class ParsedDatum extends BaseDatum {
    private final String _url;
    private final Outlink[] _outLinks;
    private final String _parsedText;

    @SuppressWarnings("unchecked")
    public ParsedDatum(String url, String parsedText, Outlink[] outLinks, Map<String, Comparable> metaData) {
        super(metaData);
        
        _url = url;
        _parsedText = parsedText;
        _outLinks = outLinks;
    }

    private Tuple convertToTuple(Outlink[] outLinks) {
        Tuple tuple = new Tuple();
        for (Outlink outlink : outLinks) {
            tuple.add(outlink.getToUrl());
            tuple.add(outlink.getAnchor());
        }
        return tuple;
    }

    public Outlink[] getOulinks() {
        return _outLinks;
    }

    public String getUrl() {
        return _url;
    }

    public String getParsedText() {
        return _parsedText;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Comparable[] getValues() {
        return new Comparable[] { _url, _parsedText, convertToTuple(_outLinks) };
    }

    public static Fields getFields() {
        return new Fields(IFieldNames.URL, IFieldNames.TEXT, IFieldNames.OUT_LINKS);
    }

}
