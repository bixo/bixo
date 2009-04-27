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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;

/**
 * Manage the set of threads that one task spawns to fetch pages.
 * 
 * @author kenkrugler
 *
 */
public class FetcherManager implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(FetcherManager.class);
    
    // TODO KKr - figure out how best to get these values, without having
    // to pass around a _conf everywhere.
    private static final int FETCH_THREAD_COUNT_CORE = 10;
    private static final int FETCH_IDLE_TIMEOUT = 1;

    private IFetchItemProvider _provider;
    private IHttpFetcherFactory _factory;
    private ThreadPoolExecutor _pool;
    private BixoFlowProcess _process;
    
    public FetcherManager(IFetchItemProvider provider, IHttpFetcherFactory factory, BixoFlowProcess process) {
        _provider = provider;
        _factory = factory;
        _process = process;

        ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(_factory.getMaxThreads() * 2);
        _pool = new ThreadPoolExecutor(Math.min(FETCH_THREAD_COUNT_CORE, _factory.getMaxThreads()),
                        _factory.getMaxThreads(), FETCH_IDLE_TIMEOUT, TimeUnit.SECONDS, queue);
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
	        
	        while (true) {
	            FetchList items = null;

	            long curTime = System.currentTimeMillis();
	            if (curTime >= nextStatusTime) {
	                nextStatusTime = curTime + (200);
	                
	                _process.setStatus(String.format("Fetching %d URLs", _process.getCounter(FetcherCounters.URLS_FETCHING)));
	            }
	            
	            // Don't bother trying to add more things to the queue if that would only throw
	            // a RejectedExecutionException.
	            if (_pool.getQueue().remainingCapacity() > 0) {
	                LOGGER.trace("Pool has capacity, requesting a FetchList from the provider");
	                items = _provider.poll();
	            } else {
	                LOGGER.trace("Pool at capacity");
	            }

	            // If we decided to check for IURLs, and we got a set to fetch from one domain...
	            if (items != null) {
                    LOGGER.trace(String.format("Creating a FetcherRunnable for %d items from %s", items.size(), items.getDomain()));
                    
	                // Create a Runnable that has a way to fetch the URLs (the IHttpFetcher), and
	                // the list of things to fetch (the <items>).
	                FetcherRunnable command = new FetcherRunnable(_factory.newHttpFetcher(), items);
	                _pool.execute(command);
	            } else {
                    LOGGER.trace("Nothing to fetch, sleeping");
	                Thread.sleep(100);
	            }
	        }
	    } catch (InterruptedException e) {
	        // ignore this one
	    } finally {
	        _pool.shutdown();
	        while (!_pool.isShutdown()) {
	            // Spin while waiting.
	            safeSleep(1000);
	            // TODO KKr - have timeout where we call _pool.terminate() to force
	            // it to shut down.
	        }
	    }
	} // run
	
	
	// TODO KKr - move to bixo utils?
	private static void safeSleep(long duration) {
	    try {
	        Thread.sleep(duration);
	    } catch (InterruptedException e) {
	        // Ignore
	    }
	}
	
	
	/**
	 * Give the caller who set up this manager a way to tell if it's appropriate to
	 * interrupt the fetching process because we're done.
	 * 
	 * @return - true if we're done fetched everything that was in process, and
	 *           there's nothing left to fetch.
	 */
	public boolean isDone() {
	    return (_pool.getActiveCount() == 0) && _provider.isEmpty();
	} // isDone
	
	
}
