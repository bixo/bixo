package bixo.fetcher.test;

import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpHandler;
import org.mortbay.http.HttpServer;

public abstract class LocalhostServerTest {

    public HttpServer startServer(HttpHandler handler, int por) throws Exception {
        HttpServer server = new HttpServer();
        server.addListener(":" + por);
        HttpContext context = server.getContext("/");
        context.addHandler(handler);
        server.start();
        return server;
    }

}
