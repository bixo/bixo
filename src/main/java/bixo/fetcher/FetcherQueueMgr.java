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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.config.QueuePolicy;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IRobotRules;
import bixo.hadoop.FetchCounters;
import cascading.tuple.TupleEntryCollector;

/**
 * Manage a set of FetcherQueue objects, one per URL grouping (either domain or IP address)
 *
 */
public class FetcherQueueMgr implements IFetchListProvider {
    public static final int DEFAULT_MAX_URLS_IN_MEMORY = 100000;
    
	private DelayQueue<FetcherQueue> _pendingQueues;	
    private Map<FetchList, FetcherQueue> _activeQueues;
    private Object _queueLock;

    private BixoFlowProcess _process;
    
	private FetcherPolicy _fetcherPolicy;
	private QueuePolicy _queuePolicy;
	
	private boolean _skipAll;
	private UrlStatus _skipStatus;
	
	private boolean _needDomains;
	private int _maxQueues;
	
    public FetcherQueueMgr(BixoFlowProcess process) {
        FetcherPolicy fetcherPolicy = new FetcherPolicy();
        QueuePolicy queuePolicy = new QueuePolicy(DEFAULT_MAX_URLS_IN_MEMORY, fetcherPolicy.getDefaultUrlsPerRequest());
        init(process, fetcherPolicy, queuePolicy);
    }
    
    public FetcherQueueMgr(BixoFlowProcess process, FetcherPolicy fetcherPolicy, QueuePolicy queuePolicy) {
        init(process, fetcherPolicy, queuePolicy);
    }
    
    private void init(BixoFlowProcess process, FetcherPolicy fetcherPolicy, QueuePolicy queuePolicy) {
        _process = process;
        _fetcherPolicy = fetcherPolicy;
        _queuePolicy = queuePolicy;
        _pendingQueues = new DelayQueue<FetcherQueue>();
        _needDomains = true;
        
        _activeQueues = new ConcurrentHashMap<FetchList, FetcherQueue>();
        _maxQueues = queuePolicy.getMaxUrlsInMemory() / queuePolicy.getMaxUrlsInMemoryPerQueue();
        _queueLock = new Object();
        
        _skipAll = false;
    }
    
	public FetcherQueue createQueue(String domain, TupleEntryCollector collector, long crawlDelay) {
	    // If the URLs we're going to be queueing don't have a specific crawl delay, or are the same
	    // as our default policy, then we can just re-use the default policy.
	    FetcherPolicy policy;
	    if ((crawlDelay == IRobotRules.UNSET_CRAWL_DELAY) || (crawlDelay == _fetcherPolicy.getCrawlDelay())) {
	        policy = _fetcherPolicy;
	    } else {
	        policy = _fetcherPolicy.makeNewPolicy(crawlDelay);
	        if (policy.getClass() != _fetcherPolicy.getClass()) {
	            // Catch case of somebody subclassing FetcherPolicy but not overriding the makeNewPolicy method
	            throw new RuntimeException("makeNewPolicy was not overridden");
	        }
	    }
	    
        return new FetcherQueue(domain, policy, _queuePolicy.getMaxUrlsInMemoryPerQueue(), collector);
	}
	
	/**
	 * Add a new queue (set of URLs for one PLD/ip address) to the set being fetched.
	 * 
	 * @param newQueue - queue to add
	 * @return - true if we were able to add it, false if at capacity
	 */
	public boolean offer(FetcherQueue newQueue) {
	    // FUTURE KKr - also limit by max # of URLs in memory?
	    if (!_needDomains || (_pendingQueues.size() >= _maxQueues)) {
	        return false;
	    }

	    // We synchronize on _queues so that we can iterate in getNextQueue without
	    // worrying about getting a ConcurrentModificationException
	    synchronized (_queueLock) {
	        _pendingQueues.add(newQueue);
	    }
	    
	    _process.increment(FetchCounters.DOMAINS_QUEUED, 1);
	    _process.increment(FetchCounters.DOMAINS_REMAINING, 1);

	    return true;
	} // offer
	
	
	/* (non-Javadoc)
	 * @see bixo.fetcher.IFetchItemProvider#isEmpty()
	 */
	public boolean isEmpty() {
	    // Note that we have to synchronize on _queues since the poll() method
	    // moves items between the pending and active queues, and release does
	    // the opposite, so w/o this we could get a false positive "is empty" result.
	    synchronized (_queueLock) {
	        return (_pendingQueues.size() == 0) && (_activeQueues.size() == 0);
	    }
	}
	
	
	/* Somebody wants URLs to fetch. See if we have a queue that contains URLs
     * that are ready to be fetched, given our fetch policy.
     * 
	 * (non-Javadoc)
	 * @see bixo.fetcher.IFetchItemProvider#poll()
	 */
	public FetchList poll() {

	    synchronized (_queueLock) {
	        FetcherQueue queue = _pendingQueues.poll();
	        if (queue != null) {
	            _needDomains = false;

	            List<ScoredUrlDatum> urls = queue.poll();
	            if (urls == null) {
	                throw new RuntimeException("Available queue has nothing to fetch!");
	            }

	            FetchList result = new FetchList(_process, queue.getCollector(), this, queue.getDomain(), urls);
	            _activeQueues.put(result, queue);
	            return result;
	        } else {
	            _needDomains = true;
	            return null;
	        }
	    }
	} // poll
	
	
    public void skipAll(UrlStatus status) {
        synchronized (_queueLock) {
            Iterator<FetcherQueue> iter = _pendingQueues.iterator();
            while (iter.hasNext()) {
                FetcherQueue queue = iter.next();
                queue.skipAll(status);
                iter.remove();
            }
            
            _skipAll = true;
            _skipStatus = status;
        }        
    }

	/**
	 * A fetcher thread has finished with <fetchList>, so we call tell
	 * the corresponding queue that it's finished, and we can move it
	 * from the set of active queues to the queue of pending queues.
	 * 
	 * @param fetchList
	 */
	public synchronized void finished(FetchList fetchList) {
	    synchronized(_queueLock) {
	        FetcherQueue queue = _activeQueues.get(fetchList);
	        if (queue == null) {
	            throw new RuntimeException("No such fetchlist: " + fetchList);
	        }

	        _activeQueues.remove(fetchList);
	        queue.release(fetchList.getUrls());

	        // As fetches are finished, if we're skipping everything that's left then
	        // don't re-queue it.
	        if (_skipAll) {
	            queue.skipAll(_skipStatus);
                _process.increment(FetchCounters.DOMAINS_FINISHED, 1);
                _process.decrement(FetchCounters.DOMAINS_REMAINING, 1);
	        } else if (!queue.isEmpty()) {
	            _pendingQueues.add(queue);
	            // TODO KKr - decrement active domains? Or still do this down lower?
	            // Feels better to do it at the same level as DOMAINS_FINISHED.
	        } else {
                _process.increment(FetchCounters.DOMAINS_FINISHED, 1);
                _process.decrement(FetchCounters.DOMAINS_REMAINING, 1);
	        }
	    }
	}
	
	
	/**
	 * Return the next entry from the priority queue.
	 * 
	 * @return next fetcher queue, based on target time to fetch.
	 */
	public FetcherQueue getNextQueue() {
	    synchronized (_queueLock) {
	        Iterator<FetcherQueue> iter = _pendingQueues.iterator();
	        if (iter.hasNext()) {
	            return iter.next();
	        } else {
	            return null;
	        }
	    }
	}
	
	public void logPendingQueues(Logger logger) {
	    logPendingQueues(logger, Integer.MAX_VALUE);
	}
	
	public void logPendingQueues(Logger logger, int numToLog) {
	    synchronized (_queueLock) {
	        Iterator<FetcherQueue> iter = _pendingQueues.iterator();
	        int curLogged = 0;
	        while ((curLogged < numToLog) && iter.hasNext()) {
	            logger.info(iter.next());
	            curLogged += 1;
	        }
	    }
	}

}
