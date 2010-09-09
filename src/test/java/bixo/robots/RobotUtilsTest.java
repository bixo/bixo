package bixo.robots;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

import bixo.fetcher.SimulationWebServerForTests;
import bixo.fetcher.http.HttpFetcher;
import bixo.utils.ConfigUtils;


public class RobotUtilsTest {

    @SuppressWarnings("serial")
    private static class CircularRedirectResponseHandler extends AbstractHttpHandler {
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            response.sendRedirect(pathInContext);
        }
    }

    @SuppressWarnings("serial")
    private static class RedirectToTopResponseHandler extends AbstractHttpHandler {
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
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
        HttpFetcher fetcher = RobotUtils.createFetcher(ConfigUtils.BIXO_TEST_AGENT, 1);
        RobotRulesParser parser = new SimpleRobotRulesParser();
        
        SimulationWebServerForTests webServer = new SimulationWebServerForTests();
        HttpServer server = webServer.startServer(new CircularRedirectResponseHandler(), 8089);
        
        try {
            RobotRules rules = RobotUtils.getRobotRules(fetcher, parser, new URL("http://localhost:8089/robots.txt"));
            Assert.assertTrue(rules.isAllowAll());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testRedirectToHtml() throws Exception {
        HttpFetcher fetcher = RobotUtils.createFetcher(ConfigUtils.BIXO_TEST_AGENT, 1);
        RobotRulesParser parser = new SimpleRobotRulesParser();
        
        SimulationWebServerForTests webServer = new SimulationWebServerForTests();
        HttpServer server = webServer.startServer(new RedirectToTopResponseHandler(), 8089);
        
        try {
            RobotRules rules = RobotUtils.getRobotRules(fetcher, parser, new URL("http://localhost:8089/robots.txt"));
            Assert.assertTrue(rules.isAllowAll());
        } finally {
            server.stop();
        }
    }

}
