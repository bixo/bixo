package bixo.tools;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;

public class RunHttpClientFetcher {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // Set up for no minimum response rate.
        FetcherPolicy policy = new FetcherPolicy();
        // TODO KKr - use real user agent here.
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, "Bixo integration test agent");

        try {
            String url = "http://telenovelas.censuratv.net/noticias/elrostrodeanaliacanal9-argentina-elrostrodeanalia-canal9/";
            FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
            System.out.println("Result = " + result.toString());
        } catch (Exception e) {
            System.out.println("Exception fetching page: " + e.getMessage());
        }
    }

}
