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
package bixo.fetcher;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.cookie.params.CookieSpecParamBean;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

@SuppressWarnings("serial")
public class HttpClientFactory implements IHttpFetcherFactory {
    transient private HttpClient _httpClient;
    private int _maxThreads;
    private HttpVersion _httpVersion;
    
    public HttpClientFactory(int maxThreads, HttpVersion httpVersion) {
        _maxThreads = maxThreads;
        _httpVersion = httpVersion;
    }
    
    public HttpClientFactory(int maxThreads) {
        this(maxThreads, HttpVersion.HTTP_1_1);
    }

    private void init() {
        // Create and initialize HTTP parameters
        HttpParams params = new BasicHttpParams();
        
        ConnManagerParams.setMaxTotalConnections(params, _maxThreads);
        // TODO KKr - get timeout from config.
        ConnManagerParams.setTimeout(params, 10000);
        // TODO KKr - setMaxConnectionsPerRoute(params, new BixoConnPerRoute())
        
        HttpProtocolParams.setVersion(params, _httpVersion);
        // TODO KKr - get user agent string from config.
        HttpProtocolParams.setUserAgent(params, "bixo");
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        // TODO KKr - what about HttpProtocolParams.setHttpElementCharset(params, "UTF-8");
        // TODO KKr - what about HttpProtocolParams.setUseExpectContinue(params, true);
        
        // TODO KKr - set on connection manager params, or client params?
        CookieSpecParamBean cookieParams = new CookieSpecParamBean(params);
        cookieParams.setSingleHeader(true);
        
        // Create and initialize scheme registry
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
        // FUTURE KKr - support https on port 443

        // Create an HttpClient with the ThreadSafeClientConnManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        _httpClient = new DefaultHttpClient(cm, params);
        
        params = _httpClient.getParams();
        // FUTURE KKr - support authentication
        HttpClientParams.setAuthenticating(params, false);
        // TODO KKr - get from config.
        HttpClientParams.setRedirecting(params, true);
        HttpClientParams.setCookiePolicy(params, CookiePolicy.BEST_MATCH);
    }

    @Override
    public IHttpFetcher newHttpFetcher() {
        if (_httpClient == null) {
            init();
        }
        
        return new HttpClientFetcher(_httpClient);
    }

    @Override
    public int getMaxThreads() {
        return _maxThreads;
    }

}
