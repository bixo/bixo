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
package bixo.fetcher;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.util.IScoreGenerator;
import bixo.utils.DiskQueue;
import bixo.utils.DomainNames;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FetcherQueue implements Delayed {
    
    private static int DEFAULT_MAX_URLS_IN_MEMORY = 100;

    private String _domain;
    private DiskQueue<ScoredUrlDatum> _queue;
    private FetcherPolicy _policy;
    private TupleEntryCollector _collector;
    private int _numActiveFetchers;
    private long _nextFetchTime;

    public FetcherQueue(String domain, FetcherPolicy policy, TupleEntryCollector collector) {
        this(domain, policy, DEFAULT_MAX_URLS_IN_MEMORY, collector);
    }
    
    // TODO KKr - get rid of use of _collector here, do skipping at higher level
    public FetcherQueue(String domain, FetcherPolicy policy, int maxUrlsInMemory, TupleEntryCollector collector) {
        _domain = domain;
        _policy = policy;
        _collector = collector;

        _numActiveFetchers = 0;
        _nextFetchTime = System.currentTimeMillis();
        _queue = new DiskQueue<ScoredUrlDatum>(maxUrlsInMemory);
    }


    /**
     * Using queue terminology, offer up <fetchItem> as something to be queued.
     * 
     * @param scoredUrlDatum - item that we'd like to have fetched. Must be valid format.
     */
    public synchronized boolean offer(ScoredUrlDatum scoredUrlDatum) {
        // TODO KKr - add lock that prevents anyone from adding new items after we've
        // started polling.

        int maxSize = _policy.getMaxUrls();
        if (maxSize == 0) {
            skip(scoredUrlDatum, UrlStatus.SKIPPED_TIME_LIMIT);
            return false;
        }

        // TODO KKr - remove this when we no longer pass skipped URLs through to the
        // FetchBuffer.
        double score = scoredUrlDatum.getScore();
        if (score == IScoreGenerator.SKIP_URL_SCORE) {
            skip(scoredUrlDatum, UrlStatus.SKIPPED_BY_SCORER);
            return false;
        }

        if (_queue.size() < maxSize) {
            _queue.add(scoredUrlDatum);
            return true;
        } else {
            // URLs come in sorted order (by score, high to low) so we can just skip
            // everything after we've hit our max limit.
            skip(scoredUrlDatum, UrlStatus.SKIPPED_BY_SCORE);
            return false;
        }
    }

    /**
     * Tell the caller whether this queue is done (empty and all using threads done)
     * 
     * @return - true if it can be disposed of safely.
     */
    public synchronized boolean isEmpty() {
        return (_numActiveFetchers == 0) && (_queue.size() == 0);
    }

    /* (non-Javadoc)
     * @see bixo.fetcher.IFetchItemProvider#poll()
     */
    public synchronized List<ScoredUrlDatum> poll() {
        // Based on our fetch policy, decide if we can return back one or more URLs to
        // be fetched.
        List<ScoredUrlDatum> result = null;
        
        if (_queue.size() == 0) {
            // Nothing to return
        } else if (_numActiveFetchers > 0) {
            // Only one active thread per domain/server at a time.
        } else if (System.currentTimeMillis() >= _nextFetchTime) {
            _numActiveFetchers += 1;
            
            FetchRequest fetchRequest = _policy.getFetchRequest(System.currentTimeMillis(), _policy.getCrawlDelay(), _queue.size());
            int numUrls = fetchRequest.getNumUrls();
            result = new ArrayList<ScoredUrlDatum>();
            for (int i = 0; i < numUrls; i++) {
                result.add(_queue.remove());
            }
            
            _nextFetchTime = fetchRequest.getNextRequestTime();
        }
        
        return result;
    } // poll

    
    public int size() {
        return _queue.size();
    }
    
    public String getDomain() {
        return _domain;
    }

    public long getNextFetchTime() {
        return _nextFetchTime;
    }
    
    public TupleEntryCollector getCollector() {
        return _collector;
    }
    
    /**
     * Return a guess as to the host name for URLs in this queue.
     * 
     * Since we typically group by IP address, we might have multiple host
     * names. And if the queue is empty, this will return whatever we get back
     * from DomainNames.safeGetHost() for an invalid URL.
     * 
     * @return host name
     */
    public String getHost() {
        String url = "";
        ScoredUrlDatum datum = _queue.peek();
        if (datum != null) {
            url = datum.getUrl();
        }
        
        return DomainNames.safeGetHost(url);
    }
    
    /**
     * We're done trying to fetch <items>
     * @param items - items previously returned from call to poll()
     */
    public synchronized void release(List<ScoredUrlDatum> urls) {
        _numActiveFetchers -= 1;
    }



    /**
     * Write all entries out as being skipped.
     */
    public synchronized void skipAll(UrlStatus status) {
        ScoredUrlDatum datum;
        while ((datum = _queue.poll()) != null) {
            skip(datum, status);
        }
    }
    
    
    private void skip(ScoredUrlDatum datum, UrlStatus status) {
        String url = datum.getUrl();
        Tuple result = new FetchedDatum(url, datum.getMetaDataMap()).toTuple();
        result.add(status.toString());
        synchronized (_collector) {
            _collector.add(result);
        }
    }


    @Override
    public long getDelay(TimeUnit timeUnit) {
        long delayInMS = _nextFetchTime - System.currentTimeMillis();
        return timeUnit.convert(delayInMS, TimeUnit.MILLISECONDS);
    }


    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     * 
     */
    @Override
    public int compareTo(Delayed other) {
        long otherTime = ((FetcherQueue)other)._nextFetchTime;
        
        if (_nextFetchTime < otherTime) {
            return -1;
        } else if (_nextFetchTime > otherTime) {
            return 1;
        } else {
            return 0;
        }
    }


    @Override
    public String toString() {
        long now = System.currentTimeMillis();
        return String.format("%s: %d URLs remaining, next fetch at %s (in %dms), delay is %dms (%d active threads)", 
                        getDomain(), _queue.size(), new Date(_nextFetchTime).toString(), _nextFetchTime - now,
                        _policy.getCrawlDelay(), _numActiveFetchers);
    }
    
    /**
     * Return guess as to when this queue would be finished.
     * 
     * Note that this is just a guess, e.g. if all remaining URLs could be fetched in
     * the next batch, then it might finish must faster.
     * 
     * @return time (in milliseconds) when queue should be done.
     */
    public long getFinishTime() {
        int numItems = _queue.size();
        long now = System.currentTimeMillis();
        if (numItems == 0) {
            // TODO KKr - this is only an OK result if num fetchers == 0, otherwise
            // finish time is # of URLs being fetched currently from this queue * some
            // value, but we don't keep track of current fetch count.
            return now;
        } else {
            FetchRequest fetchRequest = _policy.getFetchRequest(System.currentTimeMillis(), _policy.getCrawlDelay(), numItems);
            long millisecondsPerItem = (fetchRequest.getNextRequestTime() - now) / fetchRequest.getNumUrls();
            return _nextFetchTime + (millisecondsPerItem * numItems);
        }
    }
    
}
