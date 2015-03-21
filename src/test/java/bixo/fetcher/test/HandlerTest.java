/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.fetcher.test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Test;

import bixo.fetcher.simulation.SimulationWebServer;

public class HandlerTest extends SimulationWebServer {

    @Test
    public void testSimpleOpen() throws Exception {

        Server server = startServer(new EndlessRedirect(), 8089);
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

    private class EndlessRedirect extends AbstractHandler {
        @Override
        public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
            String location = pathInContext + "/something";
            response.sendRedirect(location);
            baseRequest.setHandled(true);
        }
    }

}
