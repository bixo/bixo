package bixo.fetcher;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FetcherQueue implements IFetchItemProvider {
    private String _domain;
    private List<FetchItem> _queue;
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
        _queue = new ArrayList<FetchItem>();
    }


    /**
     * Using queue terminology, offer up <url> as something to be queued.
     * 
     * @param url - URL that we'd like to have fetched. Must be valid format.
     * @param score - domain-relative score of the URL (high values => higher priority)
     * @return - true if we queued the URL
     */
    public boolean offer(URL url, float score) {
        if (_queue.size() < _maxURLs) {
            _queue.add(new FetchItem(url, score));
            _sorted = false;
            return true;
        }

        sort();

        if (score <= _queue.get(_queue.size() - 1)._score) {
            return false;
        } else {
            // Get rid of last (lowest score) item in queue, then insert
            // new item at the right location.
            _queue.remove(_queue.size() - 1);
            FetchItem fetchItem = new FetchItem(url, score);
            int index = Collections.binarySearch(_queue, fetchItem);
            if (index < 0) {
                index = -(index + 1);
            }
            _queue.add(index, fetchItem);
            return true;
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


    /**
     * We're done trying to fetch <items>
     * @param items - items previously returned from call to poll()
     */
    public synchronized void release(FetchList items) {
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
    
    public String getDomain() {
        return _domain;
    }
}
