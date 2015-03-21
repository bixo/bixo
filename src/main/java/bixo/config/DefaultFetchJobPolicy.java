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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import bixo.datum.ScoredUrlDatum;

@SuppressWarnings("serial")
public class DefaultFetchJobPolicy extends BaseFetchJobPolicy {
    
    // Max portion of available time range we'll remove each time.
    private static final long TIME_RANGE_DIVISOR = 1000;
    
    private static final int DEFAULT_MAX_URLS_PER_SERVER = Integer.MAX_VALUE;
    
    // When we have to skip URLs, how many to return in each set. We
    // want this to be artificially big, so we don't have lots and lots of sets.
    private static final int URLS_PER_SKIPPED_SET = 100;

    private int _maxUrlsPerSet;
    private int _maxUrlsPerServer;
    
    private transient long _crawlDelay;
    private transient Random _rand;
    
    private transient List<ScoredUrlDatum> _curUrls;
    private transient int _targetUrlCount;
    private transient int _totalUrls;
    private transient boolean _skipping;
    private transient long _curSortKey;

    public DefaultFetchJobPolicy() {
        this(new FetcherPolicy());
    }
    
    public DefaultFetchJobPolicy(FetcherPolicy policy) {
        this(policy.getMaxRequestsPerConnection(), DEFAULT_MAX_URLS_PER_SERVER, policy.getDefaultCrawlDelay());
    }

    public DefaultFetchJobPolicy(int maxUrlsPerSet, int maxUrlsPerServer, long defaultCrawlDelay) {
        if ((maxUrlsPerSet <= 0) || (maxUrlsPerServer <= 0)) {
            throw new IllegalArgumentException("Max URLs per set an per server must be > 0");
        }
        
        _maxUrlsPerSet = maxUrlsPerSet;
        _maxUrlsPerServer = maxUrlsPerServer;
        setDefaultCrawlDelay(defaultCrawlDelay);
    }

    @Override
    public void startFetchSet(String groupingKey, long crawlDelay) {
        _crawlDelay = crawlDelay;
        _rand = new Random();
        
        _totalUrls = 0;
        _curUrls = new ArrayList<ScoredUrlDatum>(_maxUrlsPerSet);
        _targetUrlCount = 0;
        
        _curSortKey = 0;
    }

    @Override
    public FetchSetInfo nextFetchSet(ScoredUrlDatum scoredDatum) {
        // TODO KKr - emit a result if we're switching domains.
        
        // Figure out if we're in skipping mode.
        _skipping = (_totalUrls >= getMaxUrlsPerServer(scoredDatum));
        
        // See if we need to figure out how many URLs for this this next set.
        if (_targetUrlCount == 0) {
            _curUrls.clear();
            
             if (_skipping) {
                _targetUrlCount = URLS_PER_SKIPPED_SET;
            } else {
                // Limit to default number per set, or number until we switch into skip mode.
                _targetUrlCount = 
                    Math.min(   getMaxUrlsPerSet(scoredDatum), 
                                getMaxUrlsPerServer(scoredDatum) - _totalUrls);
            }
        }
        
        _curUrls.add(scoredDatum);
        _totalUrls += 1;
 
        if (_curUrls.size() >= _targetUrlCount) {
            _targetUrlCount = 0;
            return(makeFetchSet());
        }
        
        return null;
    }

    @Override
    public FetchSetInfo endFetchSet() {
        if ((_targetUrlCount > 0) && (_curUrls.size() > 0)) {
            return makeFetchSet();
        } else {
            return null;
        }
    }

    /**
     * Make a FetchSetInfo object using our current state.
     * 
     * @return FetchSetInfo
     */
    private FetchSetInfo makeFetchSet() {
        // Trigger re-calc of target size if we do get called again.
        _targetUrlCount = 0;
        _curSortKey = nextSortKey(_rand, TIME_RANGE_DIVISOR, _curSortKey);
        long fetchDelay = _crawlDelay * _curUrls.size();
        return new FetchSetInfo(_curUrls, _curSortKey, fetchDelay, _skipping);
    }
    
    /**
     * Return max URLs per fetch job for the server indicated by the URL in <scoredDatum>.
     * 
     * @param scoredDatum datum containing URL to server
     * @return max number of URLs to fetch from the server during one job
     */
    protected int getMaxUrlsPerServer(ScoredUrlDatum scoredDatum) {
        return _maxUrlsPerServer;
    }
    
    /**
     * Return max URLs per fetch set for the server indicated by the URL in <scoredDatum>.
     * 
     * @param scoredDatum datum containing URL to server
     * @return max number of URLs to fetch from the server within a single connection
     */
    protected int getMaxUrlsPerSet(ScoredUrlDatum scoredDatum) {
        return _maxUrlsPerSet;
    }
    
    /**
     * Time to move the request time forward. We take some percentage of the remaining range (since we have
     * no way of knowing how many FetchSetDatums we'll be generating), pick a random number in
     * this range, add to our current request time, and use that as the new request time.
     * 
     * @param rand
     * @param divisor What slice of remaining time range to consume (randomly)
     * @param curRequestTime Current time (actually offset)
     * @return new request time that has been randomly moved forward.
     */
    public static long nextSortKey(Random rand, long divisor, long curRequestTime) {
        
        // We want to advance by some amount
        long targetRange = (Math.max(1, Long.MAX_VALUE - curRequestTime) / divisor) - 1;
        return curRequestTime + 1 + (Math.abs(rand.nextLong()) % targetRange);
    }
}
