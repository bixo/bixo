/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.config;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.HashSet;
import java.util.Set;

/**
 * Definition of policy for fetches.
 * 
 */
@SuppressWarnings("serial")
public class FetcherPolicy implements Serializable {
    
    public static final int NO_MIN_RESPONSE_RATE = Integer.MIN_VALUE;
    public static final int NO_REDIRECTS = 0;
    
    public static final int DEFAULT_MIN_RESPONSE_RATE = NO_MIN_RESPONSE_RATE;
    public static final int DEFAULT_MAX_CONTENT_SIZE = 64 * 1024;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 2;
    public static final int DEFAULT_MAX_REDIRECTS = 20;
    public static final String DEFAULT_ACCEPT_LANGUAGE = "en-us,en-gb,en;q=0.7,*;q=0.3";

    // Min duration (in milliseconds) between page fetches in a single fetch set.
    public static final long DEFAULT_MIN_PAGE_FETCH_INTERVAL = 1000L;

    // How long to wait before a fetch request gets rejected.
    // TODO KKr - calculate this based on the fetcher policy's max URLs/request
    private static final long DEFAULT_REQUEST_TIMEOUT = 100 * 1000L;
    
    // TODO Values between here and next dividing line should be moved
    // into DefaultFetchJobPolicy
    //
    // These are all outside of the scope of a fetching a URL
    //
    // We could potentially move all of the "fetching a URL" settings into the
    // BaseHttpFetcher class, and have DefaultHttpFetcher use them when
    // creating an HttpClient. Then we could call it DefaultFetchPolicy, versus
    // DefaultFetchJobPolicy and BaseFetchJobPolicy.
    // =========================================================
    protected static final int DEFAULT_MAX_REQUESTS_PER_CONNECTION = 10;
    
    // Interval between requests, in milliseconds.
    @Deprecated
    protected static final long DEFAULT_CRAWL_DELAY = BaseFetchJobPolicy.DEFAULT_CRAWL_DELAY;

    public static final long NO_CRAWL_END_TIME = Long.MAX_VALUE;
    public static final long DEFAULT_CRAWL_END_TIME = NO_CRAWL_END_TIME;

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

    protected long _crawlDelay;            // Delay (in milliseconds) between requests
    private int _maxRequestsPerConnection;  // Max # of URLs to request in any one connection
    private FetcherMode _fetcherMode;       // Should we skip URLs when they back up for a domain?
    private long _crawlEndTime;          // When we want the crawl to end
    private RedirectMode _redirectMode;     // What to do about redirects?
    private long _minPageFetchInterval = DEFAULT_MIN_PAGE_FETCH_INTERVAL;

    // =========================================================

    private int _minResponseRate;        // lower bounds on bytes-per-second
    private int _maxContentSize;        // Max # of bytes to use.
    private int _maxRedirects;
    private int _maxConnectionsPerHost; // 
    private String _acceptLanguage;    // What to pass for the Accept-Language request header
    private Set<String> _validMimeTypes;    // Set of mime-types that we'll accept.
    private long _requestTimeout;           // Max time for any given set of URLs (termination timeout is based on this)

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
        _validMimeTypes = new HashSet<String>();
        _maxConnectionsPerHost = DEFAULT_MAX_CONNECTIONS_PER_HOST;
        _maxRequestsPerConnection = DEFAULT_MAX_REQUESTS_PER_CONNECTION;
        _fetcherMode = FetcherMode.COMPLETE;
        _redirectMode = _maxRedirects > 0 ? RedirectMode.FOLLOW_ALL : RedirectMode.FOLLOW_NONE;
        
        _requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    }

    @Deprecated
    public long getDefaultCrawlDelay() {
        return DEFAULT_CRAWL_DELAY;
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

    @Deprecated
    public int getMaxContentSize() {
        return _maxContentSize;
    }

    @Deprecated
    public void setMaxContentSize(int maxContentSize) {
        _maxContentSize = maxContentSize;
    }
    
    /**
     * The (default) crawl delay should be specified via the BaseFetchJobPolicy, not the FetcherPolicy
     * @return
     */
    @Deprecated
    public long getCrawlDelay() {
        return _crawlDelay;
    }
    
    /**
     * The (default) crawl delay should be specified via the BaseFetchJobPolicy, not the FetcherPolicy
     * @return
     */
    @Deprecated
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
        _validMimeTypes = new HashSet<String>(validMimeTypes);
    }
    
    public void addValidMimeTypes(Set<String> validMimeTypes) {
        _validMimeTypes.addAll(validMimeTypes);
    }
    
    public void addValidMimeType(String validMimeType) {
        _validMimeTypes.add(validMimeType);
    }
    
    public RedirectMode getRedirectMode() {
        return _redirectMode;
    }
    
    public void setRedirectMode(RedirectMode mode) {
        _redirectMode = mode;
    }

    public long getRequestTimeout() {
        return _requestTimeout;
    }
    
    public void setRequestTimeout(long requestTimeout) {
        _requestTimeout = requestTimeout;
    }
    
    // TODO Move these into a CrawlPolicy
    public FetcherMode getFetcherMode() {
        return _fetcherMode;
    }
    
    public void setFetcherMode(FetcherMode mode) {
        _fetcherMode = mode;
    }
    
    /**
     * Set the minimum time (in milliseconds) between each page fetch request, when
     * fetching a FetchSet worth of URLs using a single connection. This gives you
     * more control over how hard you "hit" a site, independent of the default crawl
     * delay or the number of requests per connection.
     * 
     * @param minPageFetchInterval Minimum interval in milliseconds between requests.
     */
    public void seMinPageFetchInterval(long minPageFetchInterval) {
        _minPageFetchInterval = minPageFetchInterval;
    }
    
    public long getMinPageFetchInterval() {
        return _minPageFetchInterval;
    }
    
    /**
     * Calculate the maximum number of URLs that could be fetched in the remaining time.
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
    
    public boolean isTerminateFetch() {
        if (getCrawlEndTime() == NO_CRAWL_END_TIME) {
            return false;
        } else {
            // We're done if the current time is past the limit.
            return System.currentTimeMillis() >= getCrawlEndTime();
        }
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
