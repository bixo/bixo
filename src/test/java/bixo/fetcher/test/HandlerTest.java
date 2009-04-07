/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
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
