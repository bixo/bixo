package bixo.examples;

import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

import bixo.fetcher.simulation.SimulationWebServer;

public class SimpleCrawlServer extends SimulationWebServer {

    private HttpServer _server;
    
    public SimpleCrawlServer(AbstractHttpHandler handler, int port) throws Exception {
        _server = startServer(handler, port);
    }
    
    public void stop() {
        try {
            _server.stop();
        } catch (Exception e) {
            
        }
    }
}
