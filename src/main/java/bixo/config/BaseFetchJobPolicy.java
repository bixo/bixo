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
import java.util.List;

import bixo.datum.ScoredUrlDatum;

@SuppressWarnings("serial")
public abstract class BaseFetchJobPolicy implements Serializable {

    // Interval between requests, in milliseconds.
    public static final long UNSET_CRAWL_DELAY = Long.MIN_VALUE;
    public static final long DEFAULT_CRAWL_DELAY = 30 * 1000L;

    public static class FetchSetInfo {
        private List<ScoredUrlDatum> _urls;
        private long _sortKey;
        private long _fetchDelay;
        private boolean _skipping;
        
        public FetchSetInfo(List<ScoredUrlDatum> urls, long sortKey, long fetchDelay, boolean skipping) {
            _urls = urls;
            _sortKey = sortKey;
            _fetchDelay = fetchDelay;
            _skipping = skipping;
        }

        public List<ScoredUrlDatum> getUrls() {
            return _urls;
        }

        public long getSortKey() {
            return _sortKey;
        }

        public long getFetchDelay() {
            return _fetchDelay;
        }

        public boolean isSkipping() {
            return _skipping;
        }
    }

    private long _defaultCrawlDelay;
    
    public void setDefaultCrawlDelay(long defaultCrawlDelay) {
        _defaultCrawlDelay = defaultCrawlDelay;
    }
    
    public long getDefaultCrawlDelay() {
        return _defaultCrawlDelay;
    }
    
    // ==============================================
    // Methods used during creation of FetchSetDatums
    // ==============================================

    abstract public void startFetchSet(String groupingKey, long crawlDelay);
    abstract public FetchSetInfo nextFetchSet(ScoredUrlDatum scoredDatum);
    abstract public FetchSetInfo endFetchSet();
    
}
