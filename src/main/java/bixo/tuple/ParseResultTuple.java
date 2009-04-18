package bixo.tuple;

import bixo.IConstants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ParseResultTuple extends BaseTuple {
    public static Fields FIELDS = new Fields(IConstants.TEXT, IConstants.OUT_LINKS);

    public ParseResultTuple(String text, String[] outLinks) {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
        setText(text);
        setOutlinks(outLinks);
    }

    public void setOutlinks(String[] outLinks) {
        Tuple tuple = new Tuple();
        for (String string : outLinks) {
            tuple.add(string);
        }
        getTupleEntry().set(IConstants.OUT_LINKS, tuple);
    }

    public void setText(String text) {
        getTupleEntry().set(IConstants.TEXT, text);
    }

    public String getText() {
        return getTupleEntry().getString(IConstants.TEXT);
    }

    public String[] getOulinks() {
        Tuple t = (Tuple) getTupleEntry().get(IConstants.OUT_LINKS);
        String[] links = new String[t.size()];
        for (int i = 0; i < links.length; i++) {
            links[i] = t.getString(i);
        }
        return links;
    }

}
