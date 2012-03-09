package bixo.fetcher.simulation;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;


public class TestWebServer extends SimulationWebServer {

    private Server _server;
    
    public TestWebServer(AbstractHandler handler, int port) throws Exception {
        _server = startServer(handler, port);
    }
    
    public void stop() {
        try {
            _server.stop();
        } catch (Exception e) {
            
        }
    }

}
