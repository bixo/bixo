package bixo.items;

import bixo.Constants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class FetchItem {

    private static Fields FIELDS = new Fields(Constants.URL, Constants.SCORE);
    private Tuple _tuple;

    public FetchItem(String url, double score) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(score);
    }

    public FetchItem(Tuple tuple) {
        _tuple = tuple;
    }

    public String getUrl() {
        return _tuple.getString(FIELDS.getPos(Constants.URL));
    }

    public double getScore() {
        return _tuple.getDouble(FIELDS.getPos(Constants.SCORE));
    }

    public Tuple toTuple() {
        return _tuple;
    }

}
