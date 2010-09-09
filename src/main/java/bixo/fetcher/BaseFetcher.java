/*
 * Copyright (c) 2010 TransPac Software, Inc.
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

import java.io.Serializable;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;

@SuppressWarnings("serial")
public abstract class BaseFetcher implements Serializable {
    
    protected int _maxThreads;
    protected FetcherPolicy _fetcherPolicy;
    protected UserAgent _userAgent;
    
    public BaseFetcher(int maxThreads, FetcherPolicy fetcherPolicy, UserAgent userAgent) {
        _maxThreads = maxThreads;
        _fetcherPolicy = fetcherPolicy;
        _userAgent = userAgent;
    }

    public int getMaxThreads() {
        return _maxThreads;
    }

    public FetcherPolicy getFetcherPolicy() {
        return _fetcherPolicy;
    }

    public UserAgent getUserAgent() {
        return _userAgent;
    }

    // Return results of HTTP GET request
    public abstract FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException;
    
    public abstract void abort();
}
