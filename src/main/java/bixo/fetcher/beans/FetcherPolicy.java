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
package bixo.fetcher.beans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FetcherPolicy implements Serializable {
    public static final int NO_CRAWL_DELAY = 0;
    public static final int NO_MIN_RESPONSE_RATE = 0;
    
    public static final int DEFAULT_CRAWL_DELAY = 30;
    public static final int DEFAULT_THREADS_PER_HOST = 1;
    public static final int DEFAULT_REQUESTS_PER_CONNECTION = 10;
    public static final int DEFAULT_MIN_RESPONSE_RATE = NO_MIN_RESPONSE_RATE;
    
    private int _crawlDelay;			// Delay between requests, in seconds.
    private int _threadsPerHost;		// > 1 => ignore crawl delay
    private int _requestsPerConnection;	// > 1 => using keep-alive.
    private int _minResponseRate;        // lower bounds on bytes-per-second
    
    // TODO KKr - add RobotExclusion instance here

    public FetcherPolicy() {
        this(DEFAULT_CRAWL_DELAY, DEFAULT_THREADS_PER_HOST, DEFAULT_REQUESTS_PER_CONNECTION, DEFAULT_MIN_RESPONSE_RATE);
    }


    public FetcherPolicy(int crawlDelay, int threadsPerHost, int requestsPerConnection, int minResponseRate) {
        _crawlDelay = crawlDelay;
        _threadsPerHost = threadsPerHost;
        _requestsPerConnection = requestsPerConnection;
        _minResponseRate = minResponseRate;
    }


    public int getCrawlDelay() {
        if (_threadsPerHost > 1) {
            return 0;
        } else {
            return _crawlDelay;
        }
    }


    public void setCrawlDelay(int crawlDelay) {
        _crawlDelay = crawlDelay;
    }


    public int getThreadsPerHost() {
        return _threadsPerHost;
    }


    public void setThreadsPerHost(int threadsPerHost) {
        _threadsPerHost = threadsPerHost;
    }


    public int getRequestsPerConnection() {
        return _requestsPerConnection;
    }


    public void setRequestsPerConnect(int requestsPerConnection) {
        _requestsPerConnection = requestsPerConnection;
    }


    public int getMinResponseRate() {
        return _minResponseRate;
    }


    public void setMinResponseRate(int minResponseRate) {
        _minResponseRate = minResponseRate;
    }
    

}
