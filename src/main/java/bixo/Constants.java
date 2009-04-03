package bixo;

import cascading.tuple.Fields;

public interface Constants {

    String URL_DB = "url_db";

    String LINE = "line";

    // url DB
    String URL = "url";
    String LAST_UPDATED = "lastUpdated";
    String LAST_FETCHED = "lastFetched";
    String LAST_STATUS = "lastStatus";

    Fields URL_TUPLE_KEY = new Fields(URL);
    Fields URL_TUPLE_VALUES = new Fields(LAST_UPDATED, LAST_FETCHED, LAST_STATUS);

    String FETCH = "fetch";
    String SCORE = "score";
    String CONTENT = "content";

}
