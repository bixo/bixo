package bixo.items;

import org.apache.commons.codec.binary.Base64;

import bixo.Constants;
import bixo.fetcher.FetchContent;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class FetchedContentItem {

    private Tuple _tuple;
    private static Fields FIELDS = new Fields(Constants.URL, Constants.CONTENT);

    public FetchedContentItem() {
        _tuple = new Tuple();
    }

    public FetchedContentItem(String url, FetchContent content) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(new String(Base64.encodeBase64(content.getContent())));
    }

    public Tuple toTuple() {
        return _tuple;
    }

}
