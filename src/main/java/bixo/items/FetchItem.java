package bixo.items;

import bixo.Constants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class FetchItem implements Comparable<FetchItem> {

    private static Fields FIELDS = new Fields(Constants.URL, Constants.SCORE);
    private Tuple _tuple;
    private double _score;
    
    public FetchItem(String url, double score) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(score);
        _score = score;
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

    @Override
    public int compareTo(FetchItem o) {
        // Sort in reverse order, such that higher scores are first.
        if (_score > o._score) {
            return -1;
        } else if (_score < o._score) {
            return 1;
        } else {
            // TODO KKr - sort by URL, so that if we do a batch fetch, we're
            // fetching pages from the same area of the website.
            return 0;
        }
    }

}
