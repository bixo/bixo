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

import java.util.concurrent.ConcurrentLinkedQueue;

import cascading.tuple.TupleEntryCollector;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;

/**
 * Manage a set of FetcherQueue objects, one per URL grouping (either domain or IP address)
 *
 */
public class FetcherQueueMgr implements IFetchListProvider {
    // TODO KKr - look at using a DelayQueue here (from Concurrent package) so that
    // queues are ordered by time until they can be used again. Empty queues would
    // sort at the end, and we could check for this when getting a poll call, and
    // remove them.
	private ConcurrentLinkedQueue<FetcherQueue> _queues;	
	private BixoFlowProcess _process;
	private FetcherPolicy _defaultPolicy;
	
    public FetcherQueueMgr(BixoFlowProcess process) {
        this(process, new FetcherPolicy());
    }
    
    public FetcherQueueMgr(BixoFlowProcess process, FetcherPolicy defaultPolicy) {
        _process = process;
        _defaultPolicy = defaultPolicy;
        _queues = new ConcurrentLinkedQueue<FetcherQueue>();
    }
    
	public FetcherQueue createQueue(String domain, TupleEntryCollector collector) {
	    return new FetcherQueue(domain, _defaultPolicy, _process, collector);
	}
	
	/**
	 * Add a new queue (set of URLs for one PLD/ip address) to the set being fetched.
	 * 
	 * @param newQueue - queue to add
	 * @return - true if we were able to add it, false if at capacity
	 */
	public boolean offer(FetcherQueue newQueue) {
		// TODO KKr - return false if we've got too many URLs in memory?
		// Add at the front of the queue.
		_queues.add(newQueue);
		return true;
	} // offer
	
	
	/* (non-Javadoc)
	 * @see bixo.fetcher.IFetchItemProvider#isEmpty()
	 */
	public boolean isEmpty() {
	    // Note that we have to synchronize on _queues since the poll() method
	    // temporarily removes items from the queue and then stuffs them back at
	    // the end, so w/o this we could get a false positive "is empty" result.
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
	        int numQueues = _queues.size();

	        // We know that we might process the same queue multiple times, or not
	        // get to all of the queues, because new FetcherQueue elements are being
	        // added (or removed) by other threads at the same time...but that's OK,
	        // as processing the same FetcherQueue twice is harmless, and if we don't
	        // get to all of the queues this time (and thus return a false negative)
	        // we'll get to them the next time we get called.
	        for (int i = 0; i < numQueues; i++) {
	            FetcherQueue queue = _queues.poll();
	            if (queue == null) {
	                break;
	            }

	            if (queue.isEmpty()) {
	                // Don't put it back in the queue, as there's nothing left to
	                // do with it.
	                _process.increment(FetcherCounters.DOMAINS_FINISHED, 1);
	            } else {
	                result = queue.poll();
	                _queues.add(queue);
	                if (result != null) {
	                    break;
	                }
	            }
	        }
	    }
	    
		return result;
	} // poll
	
}
