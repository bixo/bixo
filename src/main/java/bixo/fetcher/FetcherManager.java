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

import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.fetcher.http.IHttpFetcher;
import bixo.hadoop.FetchCounters;
import bixo.utils.ThreadedExecutor;

/**
 * Manage the set of threads that one task spawns to fetch pages.
 *
 */
public class FetcherManager implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(FetcherManager.class);
    
    private static final long STATUS_UPDATE_INTERVAL = 10000;
    private static final long NO_URLS_SLEEP_TIME = 100;
    
    // Amount of time we'll wait for pending tasks to finish up
    // TODO KKr - calculate from fetcher setting.
    private static final long COMMAND_TIMEOUT = 120;

    private IFetchListProvider _provider;
    private IHttpFetcher _fetcher;
    private ThreadedExecutor _executor;
    private BixoFlowProcess _process;
    
    public FetcherManager(IFetchListProvider provider, IHttpFetcher fetcher, BixoFlowProcess process) {
        _provider = provider;
        _fetcher = fetcher;
        _process = process;
        _executor = new ThreadedExecutor(_fetcher.getMaxThreads(), COMMAND_TIMEOUT);
    }
    
    
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
	    
	    // Keep running until we're interrupted. Since the provider might be getting loaded with
	    // URLs as a rate different from our consumption, we could be ahead or behind, so we can't
	    // just terminate when there's nothing left to be fetched...more could be on the way.
	    try {
	        long nextStatusTime = 0;
	        int urlsFetching = -1;
	        int domainsFetching = -1;
	        
	        while (true) {
	            // See if we should update our status
	            int curUrlsFetching = _process.getCounter(FetchCounters.URLS_FETCHING);
	            int curDomainsFetching = _process.getCounter(FetchCounters.DOMAINS_FETCHING);
	            long curTime = System.currentTimeMillis();

	            if ((curUrlsFetching != urlsFetching) || (curDomainsFetching != domainsFetching) || (curTime >= nextStatusTime)) {
	                urlsFetching = curUrlsFetching;
	                domainsFetching = curDomainsFetching;
	                nextStatusTime = curTime + STATUS_UPDATE_INTERVAL;

	                // Figure out number of URLs remaining = queued - (fetched + fetching + failed)
	                int urlsRemaining = _process.getCounter(FetchCounters.URLS_REMAINING);
	                _process.setStatus(String.format("Fetching %d URLs from %d domains (%d URLs remaining)", urlsFetching, domainsFetching, urlsRemaining));
	            }

	            // See if we should set up the next thing to fetch
	            // TODO KKr - this creates a problem where the DOMAINS_FETCHING counter gets incremented by FetcherQueue when the call to
	            // poll() succeeds, but we can't actually start fetching it until some point later - so you wind up with an incorrect
	            // count of domains being fetched.
	            FetchList items = _provider.poll();
	            if (items != null) {
	                LOGGER.trace(String.format("Creating a FetcherRunnable for %d items from %s", items.size(), items.getDomain()));
	                
	                try {
	                    _executor.execute(new FetcherRunnable(_fetcher, items));
	                } catch (RejectedExecutionException e) {
	                    // should never happen.
	                    LOGGER.error("Fetcher handling pool rejected our request");
	                }
	            } else {
                    LOGGER.trace("Nothing to fetch, sleeping");
	                Thread.sleep(NO_URLS_SLEEP_TIME);
	            }
	        }
	    } catch (InterruptedException e) {
	        // ignore this one
	    } finally {
            try {
                if (!_executor.terminate()) {
                    LOGGER.warn("Had to do a hard shutdown of robots fetching");
                }
            } catch (InterruptedException e) {
                // TODO KKr - what to do here?
            }
	    }
	} // run
	
	
	/**
	 * Give the caller who set up this manager a way to tell if it's appropriate to
	 * interrupt the fetching process because we're done.
	 * 
	 * @return - true if we're done fetched everything that was in process, and
	 *           there's nothing left to fetch.
	 */
	public boolean isDone() {
	    return (_executor.getActiveCount() == 0) && _provider.isEmpty();
	} // isDone
	
	
}
