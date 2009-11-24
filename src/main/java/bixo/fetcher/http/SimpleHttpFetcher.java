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
package bixo.fetcher.http;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;

import org.apache.hadoop.io.BytesWritable;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.NoHttpResponseException;
import org.apache.http.ProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.params.ClientParamBean;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.cookie.params.CookieSpecParamBean;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectHandler;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.AbortedFetchException;
import bixo.exceptions.AbortedFetchReason;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.IOFetchException;
import bixo.exceptions.UrlFetchException;

@SuppressWarnings("serial")
public class SimpleHttpFetcher implements IHttpFetcher {
    private static Logger LOGGER = Logger.getLogger(SimpleHttpFetcher.class);

    // We tried 10 seconds for all of these, but got a number of connection/read timeouts for
    // sites that would have eventually worked, so bumping it up to 30 seconds.
    private static final int DEFAULT_SOCKET_TIMEOUT = 30 * 1000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
    
    private static final int DEFAULT_MAX_THREADS = 1;
    
    // This should never actually be a timeout we hit, since we manage the number of
    // fetcher threads to be <= the maxThreads value used to configure an IHttpFetcher.
    private static final long CONNECTION_POOL_TIMEOUT = 20 * 1000L;
    
    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int DEFAULT_MAX_RETRY_COUNT = 20;
    
	private static final String PERM_REDIRECT_CONTEXT_KEY = "perm-redirect";

    private static final String SSL_CONTEXT_NAMES[] = {
        "TLS",
        "Default",
        "SSL",
    };
    
    private int _maxThreads;
    private HttpVersion _httpVersion;
    private int _socketTimeout;
    private int _connectionTimeout;
    private int _maxRetryCount;
    private FetcherPolicy _fetcherPolicy;
    private UserAgent _userAgent;
    
    transient private DefaultHttpClient _httpClient;
    
    private static class MyRequestRetryHandler implements HttpRequestRetryHandler {
        private int _maxRetryCount;
        
        public MyRequestRetryHandler(int maxRetryCount) {
            _maxRetryCount = maxRetryCount;
        }
        
        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Decide about retry #" + executionCount + " for exception " + exception.getMessage());
            }
            
            if (executionCount >= _maxRetryCount) {
                // Do not retry if over max retry count
                return false;
            } else if (exception instanceof NoHttpResponseException) {
                // Retry if the server dropped connection on us
                return true;
            } else if (exception instanceof SSLHandshakeException) {
                // Do not retry on SSL handshake exception
                return false;
            }
            
            HttpRequest request = (HttpRequest)context.getAttribute(ExecutionContext.HTTP_REQUEST);
            boolean idempotent = !(request instanceof HttpEntityEnclosingRequest); 
            // Retry if the request is considered idempotent 
            return idempotent;
        }
    }
    
    /**
     * Handler to record last permanent redirect (if any) in context.
     *
     */
    private static class MyRedirectHandler extends DefaultRedirectHandler {
    	

		public MyRedirectHandler() {
    		super();
    	}
    	
    	@Override
    	public URI getLocationURI(HttpResponse response, HttpContext context) throws ProtocolException {
    		URI result = super.getLocationURI(response, context);
    		
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY) {
            	context.setAttribute(PERM_REDIRECT_CONTEXT_KEY, result);
            }

    		return result;
    	}
    }
    
    public SimpleHttpFetcher(UserAgent userAgent) {
        this(DEFAULT_MAX_THREADS, userAgent);
    }
    
    // TODO KKr - create UserAgent bean that's passed in here, which has
    // separate fields for email, web site, name.
    public SimpleHttpFetcher(int maxThreads, UserAgent userAgent) {
        this(maxThreads, new FetcherPolicy(), userAgent);
    }

    public SimpleHttpFetcher(int maxThreads, FetcherPolicy fetcherPolicy, UserAgent userAgent) {
        _maxThreads = maxThreads;
        _fetcherPolicy = fetcherPolicy;
        _userAgent = userAgent;
        
        _httpVersion = HttpVersion.HTTP_1_1;
        _socketTimeout = DEFAULT_SOCKET_TIMEOUT;
        _connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        _maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        
        // Just to be explicit, we rely on lazy initialization of this so that
        // we don't have to worry about serializing it.
        _httpClient = null;
}

    @Override
    public int getMaxThreads() {
        return _maxThreads;
    }

    @Override
    public FetcherPolicy getFetcherPolicy() {
        return _fetcherPolicy;
    }

    @Override
    public UserAgent getUserAgent() {
        return _userAgent;
    }

    public HttpVersion getHttpVersion() {
        return _httpVersion;
    }

    public void setHttpVersion(HttpVersion httpVersion) {
        if (_httpClient == null) {
            _httpVersion = httpVersion;
        } else {
            throw new IllegalStateException("Can't change HTTP version after HttpClient has been initialized");
        }
    }

    public int getSocketTimeout() {
        return _socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        if (_httpClient == null) {
            _socketTimeout = socketTimeout;
        } else {
            throw new IllegalStateException("Can't change socket timeout after HttpClient has been initialized");
        }
    }

    public int getConnectionTimeout() {
        return _connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        if (_httpClient == null) {
            _connectionTimeout = connectionTimeout;
        } else {
            throw new IllegalStateException("Can't change connection timeout after HttpClient has been initialized");
        }
    }

    public int getMaxRetryCount() {
        return _maxRetryCount;
    }
    
    public void setMaxRetryCount(int maxRetryCount) {
        _maxRetryCount = maxRetryCount;
    }
    
    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException {
        init();

        try {
        	return doGet(scoredUrl.getUrl(), scoredUrl.getMetaDataMap());
        } catch (BaseFetchException e) {
        	if (LOGGER.isTraceEnabled()) {
        		LOGGER.trace(String.format("Exception fetching %s", scoredUrl.getUrl()), e);
        	}
        	
        	throw e;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] get(String url) throws BaseFetchException {
        init();
        
        try {
            FetchedDatum result = doGet(url, new HashMap<String, Comparable>());
            return result.getContent().getBytes();
        } catch (HttpFetchException e) {
            if (e.getHttpStatus() == HttpStatus.SC_NOT_FOUND) {
                return new byte[0];
            } else {
                throw e;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private FetchedDatum doGet(String url, Map<String, Comparable> metaData) throws BaseFetchException {
        LOGGER.trace("Fetching " + url);

        HttpGet getter = null;
        HttpResponse response;
        long readStartTime;
        HttpHeaders headerMap = new HttpHeaders();
        String redirectedUrl = null;
        String newBaseUrl = null;
        boolean needAbort = false;

        try {
            getter = new HttpGet(new URI(url));
            HttpContext localContext = new BasicHttpContext();
            readStartTime = System.currentTimeMillis();
            response = _httpClient.execute(getter, localContext);

            Header[] headers = response.getAllHeaders();
            for (Header header : headers) {
                headerMap.add(header.getName(), header.getValue());
            }

            int httpStatus = response.getStatusLine().getStatusCode();
            if (httpStatus != HttpStatus.SC_OK) {
                throw new HttpFetchException(url, "Error fetching " + url, httpStatus, headerMap);
            }

            HttpHost host = (HttpHost)localContext.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
            HttpUriRequest finalRequest = (HttpUriRequest)localContext.getAttribute(ExecutionContext.HTTP_REQUEST);

            try {
                URL hostUrl = new URI(host.toURI()).toURL();
                redirectedUrl = new URL(hostUrl, finalRequest.getURI().toString()).toExternalForm();
            } catch (MalformedURLException e) {
                LOGGER.warn("Invalid host/uri specified in final fetch: " + host + finalRequest.getURI());
                redirectedUrl = url;
            }
            
            URI permRedirectUri = (URI)localContext.getAttribute(PERM_REDIRECT_CONTEXT_KEY);
            if (permRedirectUri != null) {
            	newBaseUrl = permRedirectUri.toURL().toExternalForm();
            }
        } catch (IOException e) {
            throw new IOFetchException(url, e);
        } catch (URISyntaxException e) {
            throw new UrlFetchException(url, e.getMessage());
        } catch (IllegalStateException e) {
            throw new UrlFetchException(url, e.getMessage());
        } catch (HttpFetchException e) {
            // There may be residual content, so abort the request so that the
            // connection gets returned.
            needAbort = true;
            throw e;
        } catch (Exception e) {
            // We can get things like IllegalStateException, so map all of those to
            // a generic IOFetchException
            // TODO KKr - create generic fetch exception
            throw new IOFetchException(url, new IOException(e));
        } finally {
            safeAbort(needAbort, getter);
        }

        // Figure out how much data we want to try to fetch.
        int targetLength = _fetcherPolicy.getMaxContentSize();
        boolean truncated = false;
        String contentLengthStr = headerMap.getFirst(IHttpHeaders.CONTENT_LENGTH);
        if (contentLengthStr != null) {
            try {
                int contentLength = Integer.parseInt(contentLengthStr);
                if (contentLength > targetLength) {
                    truncated = true;
                } else {
                    targetLength = contentLength;
                }
            } catch (NumberFormatException e) {
                // Ignore (and log) invalid content length values.
                LOGGER.warn("Invalid content length in header: " + contentLengthStr);
            }
        }

        // Now finally read in response body, up to targetLength bytes.
        // FUTURE KKr - use content-type to exclude/include data, as that's
        // a more accurate way to skip unwanted content versus relying on suffix.
        byte[] content = null;
        long readRate = 0;
        HttpEntity entity = response.getEntity();
        needAbort = true;

        if (entity != null) {
            InputStream in = null;

            try {
                in = entity.getContent();
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead = 0;
                int totalRead = 0;
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int readRequests = 0;
                int minResponseRate = _fetcherPolicy.getMinResponseRate();
                // TODO KKr - we need to monitor the rate while reading a
                // single block. Look at HttpClient
                // metrics support for how to do this. Once we fix this, fix
                // the test to read a smaller (< 20K)
                // chuck of data.
                while ((totalRead < targetLength) &&
                                ((bytesRead = in.read(buffer, 0, Math.min(buffer.length, targetLength - totalRead))) != -1)) {
                    readRequests += 1;
                    totalRead += bytesRead;
                    out.write(buffer, 0, bytesRead);

                    // Assume read time is at least one microsecond, to avoid DBZ exception.
                    long totalReadTime = Math.max(1, System.currentTimeMillis() - readStartTime);
                    readRate = (totalRead * 1000L) / totalReadTime;

                    // Don't bail on the first read cycle, as we can get a hiccup starting out.
                    // Also don't bail if we've read everything we need.
                    if ((readRequests > 1) && (totalRead < targetLength) && (readRate < minResponseRate)) {
                        throw new AbortedFetchException(url, AbortedFetchReason.SLOW_RESPONSE_RATE);
                    }
                }

                content = out.toByteArray();
                needAbort = truncated || (in.available() > 0);
            } catch (IOException e) {
                // We don't need to abort if there's an IOException
                throw new IOFetchException(url, e);
            } finally {
                safeAbort(needAbort, getter);
                safeClose(in);
            }
        }

        // Note that getContentType can return null.
        String contentType = (entity == null || entity.getContentType() == null) ? "" : entity.getContentType().getValue();
        FetchedDatum result = new FetchedDatum(url, redirectedUrl, System.currentTimeMillis(), headerMap, new BytesWritable(content), contentType, (int)readRate, metaData);
        result.setNewBaseUrl(newBaseUrl);
        return result;
    }
    
    private static void safeClose(Closeable o) {
        if (o != null) {
            try {
                o.close();
            } catch (Exception e) {
                // Ignore any errors
            }
        }
    }
    
    private static void safeAbort(boolean needAbort, HttpRequestBase request) {
        if (needAbort && (request != null)) {
            try {
                request.abort();
            } catch (Throwable t) {
                // Ignore any errors
            }
        }
    }

    private synchronized void init() {
        if (_httpClient == null) {
            // Create and initialize HTTP parameters
            HttpParams params = new BasicHttpParams();

            ConnManagerParams.setMaxTotalConnections(params, _maxThreads);
            
            // Set the maximum time we'll wait for a spare connection in the connection pool. We
            // shouldn't actually hit this, as we make sure (in FetcherManager) that the max number
            // of active requests doesn't exceed the value returned by getMaxThreads() here.
            ConnManagerParams.setTimeout(params, CONNECTION_POOL_TIMEOUT);
            
            // Set the socket and connection timeout to be something reasonable.
            HttpConnectionParams.setSoTimeout(params, _socketTimeout);
            HttpConnectionParams.setConnectionTimeout(params, _connectionTimeout);
            
            // Even with stale checking enabled, a connection can "go stale" between the check and the
            // next request. So we still need to handle the case of a closed socket (from the server side),
            // and disabling this check improves performance.
            HttpConnectionParams.setStaleCheckingEnabled(params, false);
            
            // We need to set up for at least the number of per-host connections as is specified
            // by our policy, plus one for slop so that we can potentially fetch robots.txt at the
            // same time as a page from the server.
            // FUTURE - set this on a per-route (host) basis when we have per-host policies for
            // doing partner crawls. We could define a BixoConnPerRoute class that supports this.
            // FUTURE - reenable threads-per-host support, if we actually need it.
            // ConnPerRouteBean connPerRoute = new ConnPerRouteBean(_fetcherPolicy.getThreadsPerHost() + 1);
            ConnPerRouteBean connPerRoute = new ConnPerRouteBean(1 + 1);
            ConnManagerParams.setMaxConnectionsPerRoute(params, connPerRoute);

            HttpProtocolParams.setVersion(params, _httpVersion);
            HttpProtocolParams.setUserAgent(params, _userAgent.getUserAgentString());
            HttpProtocolParams.setContentCharset(params, "UTF-8");
            HttpProtocolParams.setHttpElementCharset(params, "UTF-8");
            HttpProtocolParams.setUseExpectContinue(params, true);

            // TODO KKr - set on connection manager params, or client params?
            CookieSpecParamBean cookieParams = new CookieSpecParamBean(params);
            cookieParams.setSingleHeader(true);

            // Create and initialize scheme registry
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
            SSLSocketFactory sf = null;

            for (String contextName : SSL_CONTEXT_NAMES) {
                try {
                    SSLContext sslContext = SSLContext.getInstance(contextName);
                    sslContext.init(null, new TrustManager[] { new DummyX509TrustManager(null) }, null);
                    sf = new SSLSocketFactory(sslContext);
                    break;
                } catch (NoSuchAlgorithmException e) {
                    LOGGER.debug("SSLContext algorithm not available: " + contextName);
                } catch (Exception e) {
                    LOGGER.debug("SSLContext can't be initialized: " + contextName, e);
                }
            }
            
            if (sf != null) {
                sf.setHostnameVerifier(SSLSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
                schemeRegistry.register(new Scheme("https", sf, 443));
            } else {
                LOGGER.warn("No valid SSLContext found for https");
            }

            // Use ThreadSafeClientConnManager since more than one thread will be using the HttpClient.
            ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
            _httpClient = new DefaultHttpClient(cm, params);
            _httpClient.setHttpRequestRetryHandler(new MyRequestRetryHandler(_maxRetryCount));
            _httpClient.setRedirectHandler(new MyRedirectHandler());
            
            params = _httpClient.getParams();
            // FUTURE KKr - support authentication
            HttpClientParams.setAuthenticating(params, false);
            HttpClientParams.setCookiePolicy(params, CookiePolicy.BEST_MATCH);
            
            ClientParamBean clientParams = new ClientParamBean(params);
            clientParams.setHandleRedirects(_fetcherPolicy.getMaxRedirects() != FetcherPolicy.NO_REDIRECTS);
            clientParams.setMaxRedirects(_fetcherPolicy.getMaxRedirects());
        }
    }


}
