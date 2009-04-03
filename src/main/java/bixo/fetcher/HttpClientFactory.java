package bixo.fetcher;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

public class HttpClientFactory implements IHttpFetcherFactory {
    private HttpClient _httpClient;
    private int _maxThreads;
    
    public HttpClientFactory(int maxThreads) {
        _maxThreads = maxThreads;
        
        // Create and initialize HTTP parameters
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, _maxThreads);
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        // Create and initialize scheme registry 
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
        
        // Create an HttpClient with the ThreadSafeClientConnManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        _httpClient = new DefaultHttpClient(cm, params);
    }
    
    @Override
    public IHttpFetcher newHttpFetcher() {
        return new HttpClientFetcher(_httpClient);
    }

    @Override
    public int getMaxThreads() {
        return _maxThreads;
    }

}
