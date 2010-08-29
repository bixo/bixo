/*
 * Copyright (c) 2009 - 2010 TransPac Software, Inc.
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
package bixo.config;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.Set;

import bixo.fetcher.FetchRequest;

/**
 * Definition of policy for fetches.
 * 
 */
@SuppressWarnings("serial")
public class FetcherPolicy implements Serializable {
    
    // Possible redirect handling modes. If a redirect is NOT followed because of this
    // setting, then a RedirectFetchException is thrown, which is the same as what happens if
    // too many redirects occur. But RedirectFetchException now has a reason field, which
    // can be set to TOO_MANY_REDIRECTS, PERM_REDIRECT_DISALLOWED, or TEMP_REDIRECT_DISALLOWED.
    
    public enum RedirectMode {
        FOLLOW_ALL,         // Fetcher will try to follow all redirects
        FOLLOW_TEMP,        // Temp redirects are automatically followed, but not pemanent.
        FOLLOW_NONE         // No redirects are followed.
    }
    
    public enum FetcherMode {
        EFFICIENT,          // Check, and skip batch of URLs if blocked by domain still active or pending
        COMPLETE,           // Check, and queue up batch of URLs if not ready.
        IMPOLITE            // Don't check, just go ahead and process.
    }

    public static final int NO_MIN_RESPONSE_RATE = Integer.MIN_VALUE;
    public static final long NO_CRAWL_END_TIME = Long.MAX_VALUE;
    public static final int NO_REDIRECTS = 0;
    
    public static final int DEFAULT_MIN_RESPONSE_RATE = NO_MIN_RESPONSE_RATE;
    public static final int DEFAULT_MAX_CONTENT_SIZE = 64 * 1024;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 2;
    public static final long DEFAULT_CRAWL_END_TIME = NO_CRAWL_END_TIME;
    public static final int DEFAULT_MAX_REDIRECTS = 20;
    public static final String DEFAULT_ACCEPT_LANGUAGE = "en-us,en-gb,en;q=0.7,*;q=0.3";
    
    // How long to wait before a fetch request gets rejected.
    // TODO KKr - calculate this based on the fetcher policy's max URLs/request
    private static final long DEFAULT_REQUEST_TIMEOUT = 100 * 1000L;
    
    // TODO KKr - why use protected here?
    // Interval between batched fetch requests, in milliseconds.
    protected static final long DEFAULT_FETCH_INTERVAL = 5 * 60 * 1000L;
    
    protected static final int DEFAULT_MAX_REQUESTS_PER_CONNECTION = 50;
    
    public static final int NO_MAX_URLS_PER_SERVER = Integer.MAX_VALUE;
    public static final int DEFAULT_MAX_URLS_PER_SERVER = NO_MAX_URLS_PER_SERVER;
    
    // Interval between requests, in milliseconds.
    protected static final long DEFAULT_CRAWL_DELAY = 30 * 1000L;

    private int _minResponseRate;        // lower bounds on bytes-per-second
    private int _maxContentSize;        // Max # of bytes to use.
    protected long _crawlDelay;            // Delay (in milliseconds) between requests
    private int _maxRedirects;
    private int _maxConnectionsPerHost; // 
    private String _acceptLanguage;    // What to pass for the Accept-Language request header
    private Set<String> _validMimeTypes;    // Set of mime-types that we'll accept, or null
    private int _maxRequestsPerConnection;  // Max # of URLs to request in any one connection
    private long _requestTimeout;           // Max time for any given set of URLs (termination timeout is based on this)

    // TODO KKr - move these into a CrawlPolicy class, and call it CrawlMode
    private FetcherMode _fetcherMode;       // Should we skip URLs when they back up for a domain?
    private long _crawlEndTime;          // When we want the crawl to end
    private int _maxUrlsPerServer;          // Max number of URLs to fetch per server
    private RedirectMode _redirectMode;     // What to do about redirects?
    
    public FetcherPolicy() {
        this(DEFAULT_MIN_RESPONSE_RATE, DEFAULT_MAX_CONTENT_SIZE, DEFAULT_CRAWL_END_TIME, DEFAULT_CRAWL_DELAY, DEFAULT_MAX_REDIRECTS);
    }

    public FetcherPolicy(int minResponseRate, int maxContentSize, long crawlEndTime, long crawlDelay, int maxRedirects) {
        if (crawlDelay < 0) {
            throw new InvalidParameterException("crawlDelay must be >= 0: " + crawlDelay);
        }
        
        // Catch common error of specifying crawl delay in seconds versus milliseconds
        if ((crawlDelay < 100) && (crawlDelay != 0))  {
            throw new InvalidParameterException("crawlDelay must be milliseconds, not seconds: " + crawlDelay);
        }
        
        _minResponseRate = minResponseRate;
        _maxContentSize = maxContentSize;
        _crawlEndTime = crawlEndTime;
        _crawlDelay = crawlDelay;
        _maxRedirects = maxRedirects;
        
        // For rarely used parameters, we'll set it to default values and then let callers set them  individually.
        _acceptLanguage = DEFAULT_ACCEPT_LANGUAGE;
        _validMimeTypes = null;
        _maxConnectionsPerHost = DEFAULT_MAX_CONNECTIONS_PER_HOST;
        _maxRequestsPerConnection = DEFAULT_MAX_REQUESTS_PER_CONNECTION;
        _fetcherMode = FetcherMode.COMPLETE;
        _maxUrlsPerServer = DEFAULT_MAX_URLS_PER_SERVER;
        _redirectMode = _maxRedirects > 0 ? RedirectMode.FOLLOW_ALL : RedirectMode.FOLLOW_NONE;
        
        _requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    }

    /**
     * Calculate the maximum number of URLs that could be processed in the remaining time.
     * 
     * @return Number of URLs
     */
    public int getMaxUrls() {
        if (getCrawlEndTime() == NO_CRAWL_END_TIME) {
            return Integer.MAX_VALUE;
        } else {
            return calcMaxUrls();
        }
    }
    
    public long getDefaultFetchInterval() {
        return DEFAULT_FETCH_INTERVAL;
    }
    
    public int getDefaultUrlsPerRequest() {
        long crawlDelay = getDefaultCrawlDelay();
        if (crawlDelay > 0) {
            return (int)(getDefaultFetchInterval() / crawlDelay);
        } else {
            return Integer.MAX_VALUE;
        }
    }
    
    public long getDefaultCrawlDelay() {
        return DEFAULT_CRAWL_DELAY;
    }
    
    protected int calcMaxUrls() {
        if (_crawlDelay == 0) {
            return Integer.MAX_VALUE;
        } else {
            long crawlDuration = getCrawlEndTime() - System.currentTimeMillis();
            
            if (crawlDuration <= 0) {
                return 0;
            } else {
                return 1 + (int)Math.max(0, crawlDuration / _crawlDelay);
            }
        }
    }
    
    public long getCrawlEndTime() {
        return _crawlEndTime;
    }

    public void setCrawlEndTime(long crawlEndTime) {
        _crawlEndTime = crawlEndTime;
    }

    public int getMaxConnectionsPerHost() {
        return _maxConnectionsPerHost;
    }
    
    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        _maxConnectionsPerHost = maxConnectionsPerHost;
    }
    
    public int getMaxRequestsPerConnection() {
        return _maxRequestsPerConnection;
    }
    
    public void setMaxRequestsPerConnection(int maxRequestsPerConnection) {
        _maxRequestsPerConnection = maxRequestsPerConnection;
    }
    
    /**
     * Return the minimum response rate. If the speed at which bytes are being returned
     * from the server drops below this, the fetch of that page will be aborted.
     * @return bytes/second
     */
    public int getMinResponseRate() {
        return _minResponseRate;
    }

    public void setMinResponseRate(int minResponseRate) {
        _minResponseRate = minResponseRate;
    }

    public int getMaxContentSize() {
        return _maxContentSize;
    }

    public void setMaxContentSize(int maxContentSize) {
        _maxContentSize = maxContentSize;
    }
    
    public long getCrawlDelay() {
        return _crawlDelay;
    }
    
    public void setCrawlDelay(long crawlDelay) {
        _crawlDelay = crawlDelay;
    }
    
    public int getMaxRedirects() {
        return _maxRedirects;
    }
    
    public void setMaxRedirects(int maxRedirects) {
        _maxRedirects = maxRedirects;
    }
    
    public String getAcceptLanguage() {
        return _acceptLanguage;
    }
    
    public void setAcceptLanguage(String acceptLanguage) {
        _acceptLanguage = acceptLanguage;
    }
    
    public Set<String> getValidMimeTypes() {
        return _validMimeTypes;
    }
    
    public void setValidMimeTypes(Set<String> validMimeTypes) {
        _validMimeTypes = validMimeTypes;
    }
    
    public FetcherMode getFetcherMode() {
        return _fetcherMode;
    }
    
    public void setFetcherMode(FetcherMode mode) {
        _fetcherMode = mode;
    }
    
    public int getMaxUrlsPerServer() {
        return _maxUrlsPerServer;
    }
    
    public void setMaxUrlsPerServer(int maxUrlsPerServer) {
        _maxUrlsPerServer = maxUrlsPerServer;
    }
    
    public RedirectMode getRedirectMode() {
        return _redirectMode;
    }
    
    public void setRedirectMode(RedirectMode mode) {
        _redirectMode = mode;
    }

    public FetchRequest getFetchRequest(long now, long crawlDelay, int maxUrls) {
        int numUrls;
        
        if (crawlDelay > 0) {
            numUrls = Math.min(maxUrls, (int)(getDefaultFetchInterval() / crawlDelay));
        } else {
            numUrls = maxUrls;
        }
        
        numUrls = Math.min(numUrls, getMaxRequestsPerConnection());
        long nextFetchTime = now + (numUrls * crawlDelay);
        return new FetchRequest(numUrls, nextFetchTime);
    }
    
    public long getRequestTimeout() {
        return _requestTimeout;
    }
    
    public void setRequestTimeout(long requestTimeout) {
        _requestTimeout = requestTimeout;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_acceptLanguage == null) ? 0 : _acceptLanguage.hashCode());
        result = prime * result + (int) (_crawlDelay ^ (_crawlDelay >>> 32));
        result = prime * result + (int) (_crawlEndTime ^ (_crawlEndTime >>> 32));
        result = prime * result + ((_fetcherMode == null) ? 0 : _fetcherMode.hashCode());
        result = prime * result + _maxConnectionsPerHost;
        result = prime * result + _maxContentSize;
        result = prime * result + _maxRedirects;
        result = prime * result + _maxRequestsPerConnection;
        result = prime * result + _maxUrlsPerServer;
        result = prime * result + _minResponseRate;
        result = prime * result + ((_redirectMode == null) ? 0 : _redirectMode.hashCode());
        result = prime * result + (int) (_requestTimeout ^ (_requestTimeout >>> 32));
        result = prime * result + ((_validMimeTypes == null) ? 0 : _validMimeTypes.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FetcherPolicy other = (FetcherPolicy) obj;
        if (_acceptLanguage == null) {
            if (other._acceptLanguage != null)
                return false;
        } else if (!_acceptLanguage.equals(other._acceptLanguage))
            return false;
        if (_crawlDelay != other._crawlDelay)
            return false;
        if (_crawlEndTime != other._crawlEndTime)
            return false;
        if (_fetcherMode == null) {
            if (other._fetcherMode != null)
                return false;
        } else if (!_fetcherMode.equals(other._fetcherMode))
            return false;
        if (_maxConnectionsPerHost != other._maxConnectionsPerHost)
            return false;
        if (_maxContentSize != other._maxContentSize)
            return false;
        if (_maxRedirects != other._maxRedirects)
            return false;
        if (_maxRequestsPerConnection != other._maxRequestsPerConnection)
            return false;
        if (_maxUrlsPerServer != other._maxUrlsPerServer)
            return false;
        if (_minResponseRate != other._minResponseRate)
            return false;
        if (_redirectMode == null) {
            if (other._redirectMode != null)
                return false;
        } else if (!_redirectMode.equals(other._redirectMode))
            return false;
        if (_requestTimeout != other._requestTimeout)
            return false;
        if (_validMimeTypes == null) {
            if (other._validMimeTypes != null)
                return false;
        } else if (!_validMimeTypes.equals(other._validMimeTypes))
            return false;
        return true;
    }

    @Override
	public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("Crawl end time: " + getCrawlEndTime());
        result.append('\r');
        result.append("Minimum response rate: " + getMinResponseRate());
        result.append('\r');
        result.append("Maximum content size: " + getMaxContentSize());
        result.append('\r');
        result.append("Crawl delay in msec: " + getCrawlDelay());
        result.append('\r');
        result.append("Maximum redirects: " + getMaxRedirects());
        
        return result.toString();
    }
}
