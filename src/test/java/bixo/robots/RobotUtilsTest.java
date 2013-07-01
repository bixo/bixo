/*
 * Copyright 2009-2012 Scale Unlimited
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
package bixo.robots;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.Assert;

import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Test;
import org.mockito.Mockito;

import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimulationWebServerForTests;
import bixo.utils.ConfigUtils;


public class RobotUtilsTest {

    private static class CircularRedirectResponseHandler extends AbstractHandler {
        
        @Override
        public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
            response.sendRedirect(pathInContext);
        }
    }

    private static class RedirectToTopResponseHandler extends AbstractHandler {
        
        @Override
        public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
            if (pathInContext.endsWith("robots.txt")) {
                response.sendRedirect("/");
            } else {
                byte[] bytes = "<html><body></body></html>".getBytes("UTF-8");
                response.setContentLength(bytes.length);
                response.setContentType("text/html; charset=UTF-8");
                response.setStatus(200);
                
                OutputStream os = response.getOutputStream();
                os.write(bytes);
            }
        }
    }

    /**
     * Verify that when the web server has a circular redirect bug for robots.txt, we
     * treat it like "no robots".
     * 
     * @throws Exception
     */
    @Test
    public void testCircularRedirect() throws Exception {
        BaseFetcher fetcher = RobotUtils.createFetcher(ConfigUtils.BIXO_TEST_AGENT, 1);
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        
        SimulationWebServerForTests webServer = new SimulationWebServerForTests();
        Server server = webServer.startServer(new CircularRedirectResponseHandler(), 8089);
        
        try {
            BaseRobotRules rules = RobotUtils.getRobotRules(fetcher, parser, new URL("http://localhost:8089/robots.txt"));
            Assert.assertTrue(rules.isAllowAll());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testRedirectToHtml() throws Exception {
        BaseFetcher fetcher = RobotUtils.createFetcher(ConfigUtils.BIXO_TEST_AGENT, 1);
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        
        SimulationWebServerForTests webServer = new SimulationWebServerForTests();
        Server server = webServer.startServer(new RedirectToTopResponseHandler(), 8089);
        
        try {
            BaseRobotRules rules = RobotUtils.getRobotRules(fetcher, parser, new URL("http://localhost:8089/robots.txt"));
            Assert.assertTrue(rules.isAllowAll());
        } finally {
            server.stop();
        }
    }
    
    @Test
    public void testMatchAgainstEmailAddress() throws Exception {
        // The "crawler@domain.com" email address shouldn't trigger a match against the
        // "crawler" user agent name in the robots.txt file.
        final String simpleRobotsTxt = "User-agent: crawler" + "\r\n"
        + "Disallow: /";

        BaseFetcher fetcher = Mockito.mock(BaseFetcher.class);
        FetchedDatum datum = Mockito.mock(FetchedDatum.class);
        Mockito.when(datum.getContentBytes()).thenReturn(simpleRobotsTxt.getBytes());
        Mockito.when(fetcher.get(Mockito.any(ScoredUrlDatum.class))).thenReturn(datum);
        UserAgent userAgent = new UserAgent("testAgent", "crawler@domain.com", "http://www.domain.com");
        Mockito.when(fetcher.getUserAgent()).thenReturn(userAgent);
        
        URL robotsUrl = new URL("http://www.domain.com/robots.txt");
        SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
        BaseRobotRules rules = RobotUtils.getRobotRules(fetcher, parser, robotsUrl);
        
        Assert.assertTrue(rules.isAllowed("http://www.domain.com/anypage.html"));
    }
    

}
