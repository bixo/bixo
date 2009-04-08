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

import bixo.tuple.FetchTuple;

public class FetcherQueue implements IFetchItemProvider {
    private static Logger LOGGER = Logger.getLogger(FetcherQueue.class);
    
    private String _domain;
    private List<FetchTuple> _queue;
    private FetcherPolicy _policy;
    private int _numActiveFetchers;
    private long _nextFetchTime;
    private int _maxURLs;
    private boolean _sorted;

    public FetcherQueue(String domain, FetcherPolicy policy, int maxURLs) {
        _domain = domain;
        _policy = policy;
        _maxURLs = maxURLs;
        _numActiveFetchers = 0;
        _nextFetchTime = System.currentTimeMillis();
        _sorted = true;
        _queue = new ArrayList<FetchTuple>();
    }


    /**
     * Using queue terminology, offer up <url> as something to be queued.
     * 
     * @param url - URL that we'd like to have fetched. Must be valid format.
     * @param score - domain-relative score of the URL (high values => higher priority)
     * @return - true if we queued the URL
     */
    public boolean offer(String url, double score, FetchTuple fetchTuple) {
        if (_queue.size() < _maxURLs) {
            trace("adding url to unfilled queue", url);
            _queue.add(new FetchTuple(url, score));
            _sorted = false;
            return true;
        }

        // Since we have to insert, make sure the list is ordered first.
        sort();

        if (score <= _queue.get(_queue.size() - 1).getScore()) {
            trace("rejecting url due to low score", url);
            return false;
        } else {
            // Get rid of last (lowest score) item in queue, then insert
            // new item at the right location.
            trace("adding url to full queue", url);
            _queue.remove(_queue.size() - 1);
            if (fetchTuple == null) {
                fetchTuple = new FetchTuple(url, score);
            }
            
            int index = Collections.binarySearch(_queue, fetchTuple);
            if (index < 0) {
                index = -(index + 1);
            }
            
            _queue.add(index, fetchTuple);
            return true;
        }
    }

    public boolean offer(FetchTuple fetchTuple) {
        return offer(fetchTuple.getUrl(), fetchTuple.getScore(), fetchTuple);
    }
    
    public boolean offer(String url, double score) {
        return offer(url, score, null);
    }
    
    /**
     * Tell the caller whether this queue is done (empty and all using threads done)
     * 
     * @return - true if it can be disposed of safely.
     */
    public synchronized boolean isEmpty() {
        return (_numActiveFetchers == 0) && (_queue.size() == 0);
    }


    /**
     * We're done trying to fetch <items>
     * @param items - items previously returned from call to poll()
     */
    public synchronized void release(FetchList items) {
        trace("Release fetchlist", items.get(0).getUrl());
        _numActiveFetchers -= 1;
    }


    /* (non-Javadoc)
     * @see bixo.fetcher.IFetchItemProvider#poll()
     */
    public synchronized FetchList poll() {
        // Based on our fetch policy, decide if we can return back one ore more URLs to
        // be fetched.
        if (_queue.size() == 0) {
            return null;
        } else if (_policy.getThreadsPerHost() > 1) {
            // If we're not being polite, then the only limit is the
            // number of threads per host.
            if (_numActiveFetchers < _policy.getThreadsPerHost()) {
                _numActiveFetchers += 1;
                // TODO KKr - return up to the limit of our policy.
                return new FetchList(this, _queue.remove(0));
            } else {
                return null;
            }
        } else if ((_numActiveFetchers == 0) && (System.currentTimeMillis() >= _nextFetchTime)) {
            // TODO KKr - add support for _requestsPerConnection > 1 (keep-alive), by returning
            // up to that many URLs in a sequence.
            _numActiveFetchers += 1;
            _nextFetchTime = System.currentTimeMillis() + (_policy.getCrawlDelay() * 1000L);
            return new FetchList(this, _queue.remove(0));
        } else {
            return null;
        }
    } // poll


    private void sort() {
        if (!_sorted) {
            _sorted = true;
            Collections.sort(_queue);
        }
    }
    
    private void trace(String msg, String url) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("(%s) %s: %s", _domain, msg, url));
        }
    }
    
    public String getDomain() {
        return _domain;
    }
}
