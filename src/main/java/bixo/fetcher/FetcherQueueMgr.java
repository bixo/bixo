package bixo.fetcher;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manage a set of FetcherQueue objects, one per URL grouping (either PLD or IP)
 *
 */
public class FetcherQueueMgr implements IFetchItemProvider {
    // TODO KKr - look at using a DelayQueue here (from Concurrent package) so that
    // queues are ordered by time until they can be used again. Empty queues would
    // sort at the end, and we could check for this when getting a poll call, and
    // remove them.
	private ConcurrentLinkedQueue<FetcherQueue> _queues;	
	
	public FetcherQueueMgr() {
		_queues = new ConcurrentLinkedQueue<FetcherQueue>();
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
	    return _queues.size() == 0;
	}
	
	
	/* Somebody wants URLs to fetch. See if we have a queue that contains URLs
     * that are ready to be fetched, given our fetch policy.
     * 
	 * (non-Javadoc)
	 * @see bixo.fetcher.IFetchItemProvider#poll()
	 */
	public FetchList poll() {
	    FetchList result = null;
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
			} else {
				// TODO KKr - use the multi-item queue.poll call to get back
				// a list of items...might just always change it to return
				// back multi-set.
				result = queue.poll();
				_queues.add(queue);
				if (result != null) {
					break;
				}
			}
		}
		
		return result;
	} // poll
	
}
