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
package bixo.config;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FetcherPolicy implements Serializable {
    public static final int NO_CRAWL_DELAY = 0;
    public static final int NO_MIN_RESPONSE_RATE = 0;
    public static final long NO_CRAWL_END_TIME = 0;
    
    public static final int DEFAULT_CRAWL_DELAY = 30;
    public static final int DEFAULT_REQUESTS_PER_CONNECTION = 10;
    public static final int DEFAULT_MIN_RESPONSE_RATE = NO_MIN_RESPONSE_RATE;
    public static final int DEFAULT_MAX_CONTENT_SIZE = 64 * 1024;
    public static final long DEFAULT_CRAWL_END_TIME = NO_CRAWL_END_TIME;
    
    private int _crawlDelay;			// Delay between requests, in seconds.
    private int _requestsPerConnection;	// > 1 => using keep-alive.
    private int _minResponseRate;        // lower bounds on bytes-per-second
    private int _maxContentSize;        // Max # of bytes to use.
    private long _crawlEndTime;          // When we want the crawl to end
    
    // TODO KKr - add RobotExclusion instance here

    public FetcherPolicy() {
        this(DEFAULT_CRAWL_DELAY, DEFAULT_REQUESTS_PER_CONNECTION, DEFAULT_MIN_RESPONSE_RATE, DEFAULT_MAX_CONTENT_SIZE, DEFAULT_CRAWL_END_TIME);
    }


    public FetcherPolicy(int crawlDelay, int requestsPerConnection, int minResponseRate, int maxContentSize, long crawlEndTime) {
        _crawlDelay = crawlDelay;
        _requestsPerConnection = requestsPerConnection;
        _minResponseRate = minResponseRate;
        _maxContentSize = maxContentSize;
        _crawlEndTime = crawlEndTime;
    }


    public int getCrawlDelay() {
        return _crawlDelay;
    }


    public void setCrawlDelay(int crawlDelay) {
        _crawlDelay = crawlDelay;
    }


    public long getCrawlEndTime() {
        return _crawlEndTime;
    }


    public void setCrawlEndTime(long crawlEndTime) {
        _crawlEndTime = crawlEndTime;
    }


    public int getRequestsPerConnection() {
        return _requestsPerConnection;
    }


    public void setRequestsPerConnection(int requestsPerConnection) {
        _requestsPerConnection = requestsPerConnection;
    }


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
    

}
