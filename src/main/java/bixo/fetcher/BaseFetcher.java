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
package bixo.fetcher;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

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
    protected Map<String, Integer> _maxContentSizes;
    
    public BaseFetcher(int maxThreads, FetcherPolicy fetcherPolicy, UserAgent userAgent) {
        _maxThreads = maxThreads;
        _fetcherPolicy = fetcherPolicy;
        _userAgent = userAgent;
        _maxContentSizes = new HashMap<String, Integer>();
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
    
    // TODO KKr Move into a _defaultMaxContentSize field when support is removed
    // from FetcherPolicy.
    //
    @SuppressWarnings("deprecation")
    public void setDefaultMaxContentSize(int defaultMaxContentSize) {
        _fetcherPolicy.setMaxContentSize(defaultMaxContentSize);
    }
    
    @SuppressWarnings("deprecation")
    public int getDefaultMaxContentSize() {
        return _fetcherPolicy.getMaxContentSize();
    }
    
    public void setMaxContentSize(String mimeType, int maxContentSize) {
        if  (   (_fetcherPolicy.getValidMimeTypes().size() > 0)
            &&  (!(_fetcherPolicy.getValidMimeTypes().contains(mimeType)))) {
            throw new InvalidParameterException(String.format("'%s' is not a supported MIME type", mimeType));
        }
        _maxContentSizes.put(mimeType, maxContentSize);
    }

    public int getMaxContentSize(String mimeType) {
        Integer result = _maxContentSizes.get(mimeType);
        if (result == null) {
            return getDefaultMaxContentSize();
        }
        return result;
    }

    // Return results of HTTP GET request
    public abstract FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException;
    
    public abstract void abort();
}
