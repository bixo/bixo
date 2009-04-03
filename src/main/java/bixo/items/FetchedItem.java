package bixo.items;

import bixo.Constants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class FetchedItem {

    private Tuple _tuple;
    private static Fields FIELDS = new Fields(Constants.URL, Constants.CONTENT);

    public FetchedItem() {
        _tuple = new Tuple();
    }

    public FetchedItem(String url, String content) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(content);
    }

    public Tuple toTuple() {
        return _tuple;
    }

}
