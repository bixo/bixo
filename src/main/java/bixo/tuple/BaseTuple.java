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
        return getTupleEntry().toString();
    }

}
