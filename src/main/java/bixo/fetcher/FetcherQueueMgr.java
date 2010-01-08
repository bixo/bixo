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
import java.util.concurrent.DelayQueue;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.fetcher.http.IRobotRules;
import bixo.hadoop.FetchCounters;
import bixo.utils.BestDelayQueue;
import cascading.tuple.TupleEntryCollector;

/**
 * Manage a set of FetcherQueue objects, one per URL grouping (either domain or IP address)
 *
 */
public class FetcherQueueMgr implements IFetchListProvider {

    private static final int MAX_DOMAINS_IN_QUEUE = 1000;
    
	private DelayQueue<FetcherQueue> _queues;	
	private BixoFlowProcess _process;
	private FetcherPolicy _defaultPolicy;
	private boolean _needDomains;
	
    public FetcherQueueMgr(BixoFlowProcess process) {
        this(process, new FetcherPolicy());
    }
    
    public FetcherQueueMgr(BixoFlowProcess process, FetcherPolicy defaultPolicy) {
        _process = process;
        _defaultPolicy = defaultPolicy;
        _queues = new BestDelayQueue<FetcherQueue>();
        _needDomains = true;
    }
    
	public FetcherQueue createQueue(String domain, TupleEntryCollector collector, long crawlDelay) {
	    // If the URLs we're going to be queueing don't have a specific crawl delay, or are the same
	    // as our default policy, then we can just re-use the default policy.
	    // TODO KKr - use policy.equals() to decide if they are equivalent.
	    // TODO KKr - use policy.clone() to create copy, set crawl delay, as otherwise we could
	    // "lose" the custom fetcher policy that was set as the default (e.g. if adaptive was being used)
	    FetcherPolicy policy;
	    if ((crawlDelay == IRobotRules.UNSET_CRAWL_DELAY) || (crawlDelay == _defaultPolicy.getCrawlDelay())) {
	        policy = _defaultPolicy;
	    } else {
	        policy = new FetcherPolicy(_defaultPolicy.getMinResponseRate(),
	                        _defaultPolicy.getMaxContentSize(), _defaultPolicy.getCrawlEndTime(),
	                        crawlDelay, _defaultPolicy.getMaxRedirects());
	    }
	    
        return new FetcherQueue(domain, policy, _process, collector);
	}
	
	/**
	 * Add a new queue (set of URLs for one PLD/ip address) to the set being fetched.
	 * 
	 * @param newQueue - queue to add
	 * @return - true if we were able to add it, false if at capacity
	 */
	public boolean offer(FetcherQueue newQueue) {
	    // FUTURE KKr - also limit by max # of URLs in memory?
	    // FUTURE KKr - make MAX_QUEUES a configurable value.
	    if (!_needDomains || (_queues.size() >= MAX_DOMAINS_IN_QUEUE)) {
	        return false;
	    }

	    // We synchronize on _queues so that we can iterate in getLastQueue without
	    // worrying about getting a ConcurrentModificationException
	    synchronized (_queues) {
	        _queues.add(newQueue);
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
	    // temporarily removes items from the queue and then stuffs them back,
	    // so w/o this we could get a false positive "is empty" result.
	    synchronized (_queues) {
	        return _queues.size() == 0;
	    }
	}
	
	
	/* Somebody wants URLs to fetch. See if we have a queue that contains URLs
     * that are ready to be fetched, given our fetch policy.
     * 
	 * (non-Javadoc)
	 * @see bixo.fetcher.IFetchItemProvider#poll()
	 */
	public FetchList poll() {
	    FetchList result = null;
	    
	    synchronized (_queues) {
	        FetcherQueue queue;
	        while ((queue = _queues.poll()) != null) {
	            if (queue.isEmpty()) {
	                // Don't put it back in the queue, as there's nothing left to
	                // do with it. We make sure empty queues get put in the front,
	                // so that we clean them out right away.
	                _process.increment(FetchCounters.DOMAINS_FINISHED, 1);
	                _process.decrement(FetchCounters.DOMAINS_REMAINING, 1);
	            } else {
	                result = queue.poll();
	                _queues.add(queue);
	                break;
	            }
	        }
	        
	        // If we have nothing to fetch, we could use more domain queues.
	        _needDomains = (result == null);
	    }
	    
		return result;
	} // poll
	
	
	/**
	 * Figure out which remaining queue will take the longest to finish, based
	 * on the next crawl time setting, the number of URLs, and the crawl delay.
	 * 
	 * This is an expensive operation that blocks the poll request, so it should
	 * only be called occasionally.
	 * 
	 * @return queue which will end last (guesstimate), or null if no queue is pending.
	 */
	public FetcherQueue getLastQueue() {
	    FetcherQueue lastQueue = null;
	    long lastFinishTime = 0;
	    
	    synchronized (_queues) {
	        Iterator<FetcherQueue> iter = _queues.iterator();
	        while (iter.hasNext()) {
	            FetcherQueue curQueue = iter.next();
	            long curFinishTime = curQueue.getFinishTime();
	            if (curFinishTime > lastFinishTime) {
	                lastFinishTime = curFinishTime;
	                lastQueue = curQueue;
	            }
	        }
	    }

	    return lastQueue;
	}
}
