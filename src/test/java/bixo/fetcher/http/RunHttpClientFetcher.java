package bixo.fetcher.http;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;

public class RunHttpClientFetcher {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // Set up for no minimum response rate.
        FetcherPolicy policy = new FetcherPolicy();
        // TODO KKr - use real user agent here.
        IHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, "Bixo integration test agent");

        String url = "http://telenovelas.censuratv.net/noticias/elrostrodeanaliacanal9-argentina-elrostrodeanalia-canal9/";
        FetchedDatum result = fetcher.get(new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, null, 1d, null));
        FetchStatusCode statusCode = result.getStatusCode();
        System.out.println("Status = " + statusCode);
    }

}
