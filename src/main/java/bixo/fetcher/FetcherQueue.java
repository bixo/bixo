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
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.util.IScoreGenerator;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FetcherQueue implements IFetchListProvider {
    private static Logger LOGGER = Logger.getLogger(FetcherQueue.class);
    
    private String _domain;
    private List<ScoredUrlDatum> _queue;
    private FetcherPolicy _policy;
    private BixoFlowProcess _process;
    private TupleEntryCollector _collector;
    private int _numActiveFetchers;
    private long _nextFetchTime;
    private boolean _sorted;
    private int _numQueued;
    private int _numSkipped;

    public FetcherQueue(String domain, FetcherPolicy policy, BixoFlowProcess process, TupleEntryCollector collector) {
        _domain = domain;
        _policy = policy;
        _process = process;
        _collector = collector;

        _numActiveFetchers = 0;
        _nextFetchTime = System.currentTimeMillis();
        _sorted = true;
        _queue = new ArrayList<ScoredUrlDatum>();

        _numQueued = 0;
        _numSkipped = 0;
        
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Setting up queue for %s with next fetch time of %d", _domain, _nextFetchTime));
        }
    }


    /**
     * Using queue terminology, offer up <fetchItem> as something to be queued.
     * 
     * @param scoredUrlDatum - item that we'd like to have fetched. Must be valid format.
     */
    public synchronized void offer(ScoredUrlDatum scoredUrlDatum) {
        // TODO KKr - add lock that prevents anyone from adding new items after we've
        // started polling.

        int maxSize = _policy.getMaxUrls();
        if (maxSize == 0) {
            _numSkipped += 1;
            skip(scoredUrlDatum, UrlStatus.SKIPPED_TIME_LIMIT);
            return;
        }

        // TODO KKr - remove this when we no longer pass skipped URLs through to the
        // FetchBuffer.
        double score = scoredUrlDatum.getScore();
        if (score == IScoreGenerator.SKIP_URL_SCORE) {
            _numSkipped += 1;
            skip(scoredUrlDatum, UrlStatus.SKIPPED_BY_SCORER);
            return;
        }
        
        // See if we can just add without worrying about sorting.
        if (_queue.size() < maxSize) {
            _numQueued += 1;
            _queue.add(scoredUrlDatum);
            _sorted = false;
            return;
        }

        // Since we have to insert, make sure the list is ordered first.
        sort();

        // TODO KKr - should we trim the queue? Given current time, we might have
        // more than getMaxUrls in the queue already.
        if (score <= _queue.get(_queue.size() - 1).getScore()) {
            _numSkipped += 1;
            skip(scoredUrlDatum, UrlStatus.SKIPPED_BY_SCORE);
            return;
        }

        // Get rid of last (lowest score) item in queue, then insert
        // new item at the right location. Don't do any adjustment of
        // queued count, as a previously queued entry has been replaced.
        _numSkipped += 1;
        ScoredUrlDatum lastEntry = _queue.remove(_queue.size() - 1);
        skip(lastEntry, UrlStatus.SKIPPED_BY_SCORE);

        int index = Collections.binarySearch(_queue, scoredUrlDatum);
        if (index < 0) {
            index = -(index + 1);
        }

        _queue.add(index, scoredUrlDatum);
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
    public synchronized FetchList poll() {
        // Based on our fetch policy, decide if we can return back one ore more URLs to
        // be fetched.
        FetchList result = null;
        
        if (_queue.size() == 0) {
            // Nothing to return
        } else if ((_policy.getCrawlEndTime() != FetcherPolicy.NO_CRAWL_END_TIME) && (System.currentTimeMillis() >= _policy.getCrawlEndTime())) {
            // We're past the end of the target fetch window, so bail.
            skipAll(UrlStatus.SKIPPED_TIME_LIMIT);
        } else if ((_numActiveFetchers == 0) && (System.currentTimeMillis() >= _nextFetchTime)) {
            _numActiveFetchers += 1;
            
            // Make sure we return things in sorted order
            sort();

            FetchRequest fetchRequest = _policy.getFetchRequest(_queue.size());
            int numUrls = fetchRequest.getNumUrls();
            result = new FetchList(this, _process, _collector);
            result.addAll(_queue.subList(0, numUrls));
            _queue.subList(0, numUrls).clear();
            
            _nextFetchTime = fetchRequest.getNextRequestTime();
            
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Return list for %s with next fetch time of %d", _domain, _nextFetchTime));
            }
        }
        
        if (result != null) {
            _process.increment(FetcherCounters.DOMAINS_FETCHING, 1);
        }
        
        return result;
    } // poll

    
    public int size() {
        return _queue.size();
    }
    
    public int getNumSkipped() {
        return _numSkipped;
    }
    
    public int getNumQueued() {
        return _numQueued;
    }
    
    /**
     * We're done trying to fetch <items>
     * @param items - items previously returned from call to poll()
     */
    public synchronized void release(FetchList items) {
        _process.decrement(FetcherCounters.DOMAINS_FETCHING, 1);
        _numActiveFetchers -= 1;
    }



    private void sort() {
        if (!_sorted) {
            _sorted = true;
            Collections.sort(_queue);
        }
    }
    
    public String getDomain() {
        return _domain;
    }


    /**
     * Write all entries out as being skipped.
     */
    public synchronized void skipAll(UrlStatus status) {
        for (ScoredUrlDatum datum : _queue) {
            skip(datum, status);
        }
        _queue.clear();
    }
    
    private void skip(ScoredUrlDatum datum, UrlStatus status) {
        String url = datum.getUrl();
        Tuple result = new FetchedDatum(url, datum.getMetaDataMap()).toTuple();
        result.add(status.toString());
        synchronized (_collector) {
            _collector.add(result);
        }
    }
    
}
