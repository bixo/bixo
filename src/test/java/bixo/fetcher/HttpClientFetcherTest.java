package bixo.fetcher;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpServer;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.HttpClientFactory;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.simulation.SimulationWebServer;

public class HttpClientFetcherTest extends SimulationWebServer {

    @Test
    public final void testSlowServerTermination() throws Exception {
        // Need to read in more than 2 8K blocks currently, due to how
        // HttpClientFetcher
        // is designed...so use 20K bytes. And the duration is 2 seconds, so 10K
        // bytes/sec.
        HttpServer server = startServer(new SlowResponseHandler(20000, 2 * 1000L), 8089);

        // Set up for a minimum response rate of 20000 bytes/second.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(20000);

        HttpClientFactory factory = new HttpClientFactory(1, policy);
        IHttpFetcher fetcher = factory.newHttpFetcher();

        FetchedDatum result = fetcher.get(new ScoredUrlDatum("http://localhost:8089/test.html", 0, 0, FetchStatusCode.NEVER_FETCHED, null, 1d, null));
        server.stop();

        // Since our SlowResponseHandler is returning 10000 bytes in 1 second,
        // we should
        // get a aborted result.
        FetchStatusCode statusCode = result.getStatusCode();
        Assert.assertEquals(FetchStatusCode.ABORTED, statusCode);
    }

    @Test
    public final void testNotTerminatingSlowServers() throws Exception {
        // Return 20000 bytes at 10000 bytes/second - would normally trigger an
        // error.
        HttpServer server = startServer(new SlowResponseHandler(1000, 500), 8089);

        // Set up for no minimum response rate.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(FetcherPolicy.NO_MIN_RESPONSE_RATE);

        HttpClientFactory factory = new HttpClientFactory(1, policy);
        IHttpFetcher fetcher = factory.newHttpFetcher();

        FetchedDatum result = fetcher.get(new ScoredUrlDatum("http://localhost:8089/test.html", 0, 0, FetchStatusCode.NEVER_FETCHED, null, 1d, null));
        server.stop();

        FetchStatusCode statusCode = result.getStatusCode();
        Assert.assertEquals(FetchStatusCode.FETCHED, statusCode);
    }
}
