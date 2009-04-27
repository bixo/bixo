package bixo.tuple;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class BaseTuple {
    private TupleEntry _tupleEntry;

    public BaseTuple(TupleEntry entry) {
        _tupleEntry = entry;
    }

    public Tuple toTuple() {
        return _tupleEntry.getTuple();
    }

    protected TupleEntry getTupleEntry() {
        return _tupleEntry;
    }

    @Override
    public String toString() {
        // TODO KKr - currently cascading will print out unlimited amounts of data for the tuple, which
        // sucks when you're watching on the console. Limit the total data, and do a better job of screening
        // out invalid character codes, as currently he'll try to print control codes (which clear the console)
        return getTupleEntry().toString();
    }

}
