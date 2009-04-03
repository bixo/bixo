package bixo.fetcher;

import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class HttpClientFetcher implements IHttpFetcher {
    private static Logger LOGGER = Logger.getLogger(HttpClientFetcher.class);
    
    private HttpClient _httpClient;
    private HttpContext _httpContext;
    
    public HttpClientFetcher(HttpClient httpClient) {
        _httpClient = httpClient;
        _httpContext = new BasicHttpContext();
    }
    
    
    @Override
    public FetchResult get(String url) {
        HttpGet httpget = null;
        
        try {
            LOGGER.trace("Fetching " + url);
            httpget = new HttpGet(new URI(url));
            HttpResponse response = _httpClient.execute(httpget, _httpContext);
            HttpEntity entity = response.getEntity();
            byte[] bytes = EntityUtils.toByteArray(entity);
            // TODO KKr - handle redirects, real content type, what about charset? Do we need to capture HTTP headers?
            return new FetchResult(new FetchStatusCode(0), new FetchContent(url, url, System.currentTimeMillis(), bytes, null));
        } catch (Throwable t) {
            safeAbort(httpget);
            
            LOGGER.debug("Exception while fetching url " + url + ": " + t.getMessage(), t);
            // TODO KKr - use real status for exception, include exception msg somehow.
            
            return new FetchResult(new FetchStatusCode(-1), new FetchContent(url, url, System.currentTimeMillis(), null, null));
        }

    }

    private static void safeAbort(HttpRequestBase request) {
        try {
            request.abort();
        } catch (Throwable t) {
            // Ignore any errors
        }
    }

}
