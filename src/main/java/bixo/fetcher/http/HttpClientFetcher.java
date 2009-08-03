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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.cookie.params.CookieSpecParamBean;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.ScoredUrlDatum;

@SuppressWarnings("serial")
public class HttpClientFetcher implements IHttpFetcher {
    private static Logger LOGGER = Logger.getLogger(HttpClientFetcher.class);

    // We tried 10 seconds for all of these, but got a number of connection/read timeouts for
    // sites that would have eventually worked, so bumping it up to 30 seconds.
    private static final int DEFAULT_SOCKET_TIMEOUT = 30 * 1000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
    
    // This should never actually be a timeout we hit, since we manage the number of
    // fetcher threads to be <= the maxThreads value used to configure an IHttpFetcher.
    private static final long CONNECTION_POOL_TIMEOUT = 20 * 1000L;
    
    private static final int ERROR_CONTENT_LENGTH = 1024;
    public static final int BUFFER_SIZE = 8 * 1024;

    private int _maxThreads;
    private HttpVersion _httpVersion;
    private int _socketTimeout;
    private int _connectionTimeout;
    private FetcherPolicy _fetcherPolicy;
    private String _userAgent;
    
    transient private HttpClient _httpClient;
    
    // TODO KKr - create UserAgent bean that's passed in here, which has
    // separate fields for email, web site, name.
    public HttpClientFetcher(int maxThreads, String userAgent) {
        this(maxThreads, new FetcherPolicy(), userAgent);
    }

    public HttpClientFetcher(int maxThreads, FetcherPolicy fetcherPolicy, String userAgent) {
        _maxThreads = maxThreads;
        _fetcherPolicy = fetcherPolicy;
        _userAgent = userAgent;
        
        _httpVersion = HttpVersion.HTTP_1_1;
        _socketTimeout = DEFAULT_SOCKET_TIMEOUT;
        _connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        
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

    public String getUserAgent() {
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

    @SuppressWarnings("unchecked")
    private FetchedDatum doGet(String url, Map<String, Comparable> metaData) throws IOException, URISyntaxException {
        LOGGER.trace("Fetching " + url);
        HttpGet getter = new HttpGet(new URI(url));
        int httpStatus = FetchedDatum.SC_UNKNOWN;
        
        try {
            long readStartTime = System.currentTimeMillis();
            HttpResponse response = _httpClient.execute(getter);
            httpStatus = response.getStatusLine().getStatusCode();

            // Figure out how much data we want to try to fetch.
            int targetLength;
            FetchStatusCode fsCode;
            if (httpStatus == HttpStatus.SC_OK) {
                fsCode = FetchStatusCode.FETCHED;
                targetLength = _fetcherPolicy.getMaxContentSize();
            } else {
                fsCode = FetchStatusCode.ERROR;

                // Even for an error case, we can use the response body data for debugging.
                targetLength = ERROR_CONTENT_LENGTH;
            }

            HttpHeaders headerMap = new HttpHeaders();
            Header[] headers = response.getAllHeaders();
            for (Header header : headers) {
                headerMap.add(header.getName(), header.getValue());
            }

            // Get the length from the headers. If we don't get a length, not sure what
            // the right thing to do is.
            String contentLength = headerMap.getFirst(IHttpHeaders.CONTENT_LENGTH);
            if (contentLength != null) {
                try {
                    targetLength = Math.min(targetLength, Integer.parseInt(contentLength));
                } catch (NumberFormatException e) {
                    // Ignore (and log) invalid content length values.
                    LOGGER.warn("Invalid content length in header: " + contentLength);
                }
            }

            // Now finally read in response body, up to targetLength bytes.
            // FUTURE KKr - use content-type to exclude/include data, as that's
            // a more accurate way to skip unwanted content versus relying on suffix.
            byte[] content = null;
            long readRate = 0;
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                InputStream in = entity.getContent();

                try {
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
                    while ((bytesRead = in.read(buffer, 0, Math.min(buffer.length, targetLength - totalRead))) != -1) {
                        readRequests += 1;
                        totalRead += bytesRead;
                        out.write(buffer, 0, bytesRead);

                        // Assume read time is at least one microsecond, to avoid DBZ exception.
                        long totalReadTime = Math.max(1, System.currentTimeMillis() - readStartTime);
                        readRate = (totalRead * 1000L) / totalReadTime;

                        // Don't bail on the first read cycle, as we can get a hiccup starting out.
                        // Also don't bail if we've read everything we need.
                        if ((readRequests > 1) && (totalRead < targetLength) && (readRate < minResponseRate)) {
                            fsCode = FetchStatusCode.ABORTED;
                            safeAbort(getter);
                            break;
                        }
                        
                        // Do explicit abort if we're truncating, as that's the only safe way to terminate
                        // a keep-alive connection.
                        if (totalRead >= targetLength) {
                            safeAbort(getter);
                            break;
                        }
                    }

                    content = out.toByteArray();
                } catch (IOException e) {
                    // We don't need to abort if there's an IOException
                    
                    if (fsCode == FetchStatusCode.FETCHED) {
                        throw e;
                    } else {
                        // Ignore exceptions that happen while we're just reading in content for the fetch failed case.
                    }
                } catch (RuntimeException e) {
                    safeAbort(getter);
                    
                    if (fsCode == FetchStatusCode.FETCHED) {
                        throw e;
                    } else {
                        // Ignore exceptions that happen while we're just reading in content for the fetch failed case.
                    }
                } finally {
                    // Make sure the connection is released immediately.
                    safeClose(in);
                }
            }

            // Note that getContentType can return null, e.g. if we got a 404 (not found) error.
            String contentType = entity.getContentType() == null ? null : entity.getContentType().getValue();

            // TODO KKr - handle redirects, real content type, what about
            // charset? Do we need to capture HTTP headers?
            String redirectedUrl = url;
            // TODO SG used the new enum here.Use different status than fetch if
            // you need to.
            return new FetchedDatum(fsCode, httpStatus, url, redirectedUrl, System.currentTimeMillis(), headerMap, new BytesWritable(content), contentType, (int)readRate, metaData);
        } catch (Exception e) {
            LOGGER.debug("Exception while fetching url " + url, e);
            // TODO KKr - use real status for exception, include exception msg somehow. Could create a more
            // generic fetch status, that has a fetch status code, http status, message, etc. and a call to
            // return true if the fetch succeeded, and another to return true if the fetch failed. Both might
            // be false if the fetch was aborted, for example. Cheesy hack is to use a special header value
            // that has the exception in it. Though putting errors inside of "fetched datum" seems a bit odd
            // to begin with, as this is something that hasn't actually been fetched.

            return FetchedDatum.createErrorDatum(url, e.getClass().getSimpleName() + ": " + e.getMessage(), metaData);
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) {
        init();
        
        String msg = "";
        String url = scoredUrl.getNormalizedUrl();
        Map<String, Comparable> metaData = scoredUrl.getMetaDataMap();
        
        // Because of potentially stale connections, we need to retry if we get an IOException
        for (int i = 0; i < 2; i++) {
            try {
                return doGet(url, metaData);
            } catch (IOException e) {
                // Ignore, so we'll try N times.
                LOGGER.debug("Retrying HTTP GET request - potentially stale connection");
                msg = e.getMessage();
            } catch (URISyntaxException e) {
                return FetchedDatum.createErrorDatum(url, e.getMessage(), metaData);
            }
        }
        
        return FetchedDatum.createErrorDatum(url, msg, metaData);
    }

    private static void safeClose(Closeable o) {
        try {
            o.close();
        } catch (Exception e) {
            // Ignore any errors
        }
    }
    
    private static void safeAbort(HttpRequestBase request) {
        try {
            request.abort();
        } catch (Throwable t) {
            // Ignore any errors
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
            
            HttpProtocolParams.setUserAgent(params, _userAgent);
            HttpProtocolParams.setContentCharset(params, "UTF-8");
            HttpProtocolParams.setHttpElementCharset(params, "UTF-8");
            HttpProtocolParams.setUseExpectContinue(params, true);

            // TODO KKr - set on connection manager params, or client params?
            CookieSpecParamBean cookieParams = new CookieSpecParamBean(params);
            cookieParams.setSingleHeader(true);

            // Create and initialize scheme registry
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
            // FUTURE KKr - support https on port 443

            // Use ThreadSafeClientConnManager since more than one thread will be using the HttpClient.
            ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
            _httpClient = new DefaultHttpClient(cm, params);

            params = _httpClient.getParams();
            // FUTURE KKr - support authentication
            HttpClientParams.setAuthenticating(params, false);
            // TODO KKr - get from fetch policy
            HttpClientParams.setRedirecting(params, true);
            HttpClientParams.setCookiePolicy(params, CookiePolicy.BEST_MATCH);
        }
    }


}
