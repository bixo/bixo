package bixo.items;

import bixo.Constants;
import bixo.fetcher.FetchStatusCode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class FetchedUrlItem {

    private Tuple _tuple;
    private static Fields FIELDS = new Fields(Constants.URL, Constants.STATUS);

    public FetchedUrlItem() {
        _tuple = new Tuple();
    }

    public FetchedUrlItem(String url, FetchStatusCode status) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(status.getCode());
    }

    public Tuple toTuple() {
        return _tuple;
    }

}
