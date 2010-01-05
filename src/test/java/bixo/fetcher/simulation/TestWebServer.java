package bixo.fetcher.simulation;

import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

public class TestWebServer extends SimulationWebServer {

    private HttpServer _server;
    
    public TestWebServer(AbstractHttpHandler handler, int port) throws Exception {
        _server = startServer(handler, port);
    }
    
    public void stop() {
        try {
            _server.stop();
        } catch (Exception e) {
            
        }
    }

}
