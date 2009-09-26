package bixo.tools;

import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;

public class RunHttpClientFetcher {

    /**
     * @param args - URL to fetch
     */
    public static void main(String[] args) {
    	// Use standard Firefox agent name, as some sites won't work w/non-standard names.
        IHttpFetcher fetcher = new SimpleHttpFetcher("Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.8) Gecko/2009032608 Firefox/3.0.8");

        try {
            String url = args[0];
            FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
            System.out.println("Result = " + result.toString());
        } catch (Exception e) {
            System.out.println("Exception fetching page: " + e.getMessage());
        }
    }

}
