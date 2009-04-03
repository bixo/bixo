package bixo.items;

import bixo.Constants;
import bixo.fetcher.FetchStatus;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Just a wrapper around tuple with getter and setter for the known fields
 * 
 */
public class UrlItem {

    private final Tuple _tuple;
    public static Fields FIELDS = new Fields(Constants.URL, Constants.LAST_UPDATED, Constants.LAST_FETCHED, Constants.LAST_STATUS);

    public UrlItem(String url, long lastUpdated, long lastFetched, FetchStatus status) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(lastUpdated);
        _tuple.add(lastFetched);
        _tuple.add(status.ordinal());
    }

    public UrlItem(Tuple tuple) {
        _tuple = tuple;
    }

    public String getUrl() {
        return _tuple.getString(FIELDS.getPos(Constants.URL));
    }

    public long getLastFetched() {
        return _tuple.getLong(FIELDS.getPos(Constants.LAST_FETCHED));
    }

    public Tuple toTuple() {
        return _tuple;
    }
}
