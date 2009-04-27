package bixo.fetcher;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

import bixo.fetcher.beans.FetchStatusCode;
import bixo.fetcher.beans.FetcherPolicy;
import bixo.fetcher.simulation.SimulationWebServer;
import bixo.tuple.FetchResultTuple;


public class HttpClientFetcherTest extends SimulationWebServer {
    
    @SuppressWarnings("serial")
    private class SlowResponseHandler extends AbstractHttpHandler {
        private int _length;
        private long _duration;
        
        public SlowResponseHandler(int length, long duration) {
            _length = length;
            _duration = duration;
        }
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            response.setContentLength(_length);
            response.setContentType("text/html");
            response.setStatus(200);
            
            OutputStream os = response.getOutputStream();
            long startTime = System.currentTimeMillis();
            
            try {
                for (long i = 0; i < _length; i++) {
                    os.write(0);
                    
                    // Given i/_length as % of data written, we know that
                    // this * duration is the target elapsed time. Figure out
                    // how much to delay to make it so.
                    long curTime = System.currentTimeMillis();
                    long elapsedTime = curTime - startTime;
                    long targetTime = (i * _duration) / (long)_length;
                    if (elapsedTime < targetTime) {
                        Thread.sleep(targetTime - elapsedTime);
                    }
                }
            } catch (InterruptedException e) {
                throw new HttpException(500, "Response handler interrupted");
            }
        }
    }

    
    @Test
    public final void testSlowServerTermination() throws Exception {
        // Need to read in more than 2 8K blocks currently, due to how HttpClientFetcher
        // is designed...so use 20K bytes. And the duration is 2 seconds, so 10K bytes/sec.
        HttpServer server = startServer(new SlowResponseHandler(20000, 2 * 1000L), 8089);

        // Set up for a minimum response rate of 20000 bytes/second.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(20000);
        
        HttpClientFactory factory = new HttpClientFactory(1, policy);
        IHttpFetcher fetcher = factory.newHttpFetcher();
        
        FetchResultTuple result = fetcher.get("http://localhost:8089/test.html");
        server.stop();

        // Since our SlowResponseHandler is returning 10000 bytes in 1 second, we should
        // get a aborted result.
        FetchStatusCode statusCode = result.getStatusCode();
        Assert.assertEquals(FetchStatusCode.ABORTED, statusCode);
    }
    
    @Test
    public final void testNotTerminatingSlowServers() throws Exception {
        // Return 20000 bytes at 10000 bytes/second - would normally trigger an error.
        HttpServer server = startServer(new SlowResponseHandler(1000, 500), 8089);
        
        // Set up for no minimum response rate.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMinResponseRate(FetcherPolicy.NO_MIN_RESPONSE_RATE);
        
        HttpClientFactory factory = new HttpClientFactory(1, policy);
        IHttpFetcher fetcher = factory.newHttpFetcher();
        
        FetchResultTuple result = fetcher.get("http://localhost:8089/test.html");
        server.stop();

        FetchStatusCode statusCode = result.getStatusCode();
        Assert.assertEquals(FetchStatusCode.FETCHED, statusCode);
    }
}
