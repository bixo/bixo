package bixo.fetcher.test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

public class HandlerTest extends LocalhostServerTest {

    @Test
    public void testSimpleOpen() throws Exception {

        HttpServer server = startServer(new EndlessRedirect(), 8089);

        HttpURLConnection.setFollowRedirects(false);

        int count = 0;
        String location = "something";
        while (count < 10) {
            URL url = new URL("http://127.0.0.1:8089/" + location);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            String header = connection.getHeaderField("location");
            if (header != null) {
                count++;
                location = header;
            } else {
                throw new RuntimeException("Expected redirect header not found");
            }
        }
        System.out.println("num of redirects: " + count);
        server.stop();
    }

    @SuppressWarnings("serial")
    private class EndlessRedirect extends AbstractHttpHandler {
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            String location = pathInContext + "/something";
            response.sendRedirect(location);
        }
    }

}
