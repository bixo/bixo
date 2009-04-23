package bixo.fetcher;

import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;

import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

import bixo.fetcher.beans.FetchStatusCode;
import bixo.tuple.FetchResultTuple;


public class HttpClientFetcherTest extends TestCase {
    
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
            long targetEndTime = System.currentTimeMillis() + _duration;
            
            try {
                for (int i = 0; i < _length; i++) {
                    os.write(0);
                    
                    // We want to delay by (remaining time until target)/(remaining bytes)
                    long msDelay = Math.max(0, (targetEndTime - System.currentTimeMillis())/(_length - i));
                    wait(msDelay);
                }
            } catch (InterruptedException e) {
                throw new HttpException(500, "Response handler interrupted");
            }
        }
    }

    
    @Test
    public final void testSlowServerTermination() throws Exception {
        SimulationWebServerForTests simServer = new SimulationWebServerForTests();
        HttpServer server = simServer.startServer(new SlowResponseHandler(10000, 10 * 1000L), 8089);

        HttpClientFactory factory = new HttpClientFactory(1);
        IHttpFetcher fetcher = factory.newHttpFetcher();
        
        FetchResultTuple result = fetcher.get("http://domain.com:8089/test.html");
        server.stop();

        FetchStatusCode statusCode = result.getStatusCode();
        assertEquals(FetchStatusCode.ERROR, statusCode.ordinal());
    }
}
