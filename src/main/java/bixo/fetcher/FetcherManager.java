package bixo.fetcher;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * Manage the set of threads that one task spawns to fetch pages.
 * 
 * @author kenkrugler
 *
 */
public class FetcherManager implements Runnable {
    private static Logger LOGGER = Logger.getLogger(FetcherManager.class);
    
    // TODO KKr - figure out how best to get these values, without having
    // to pass around a conf everywhere.
    private static final int FETCH_THREAD_COUNT_CORE = 10;
    private static final int FETCH_IDLE_TIMEOUT = 1;

    private IFetchItemProvider _provider;
    private IHttpFetcherFactory _factory;
    private ThreadPoolExecutor _pool;
    private FetchCollector _collector;
    
    public FetcherManager(IFetchItemProvider provider, IHttpFetcherFactory factory, FetchCollector collector) {
        _provider = provider;
        _factory = factory;
        _collector = collector;
        
        ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(_factory.getMaxThreads() * 2);
        _pool = new ThreadPoolExecutor(FETCH_THREAD_COUNT_CORE, _factory.getMaxThreads(), FETCH_IDLE_TIMEOUT, TimeUnit.SECONDS, queue);
    }
    
    
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
	    
	    // Keep running until we're interrupted. Since the provider might be getting loaded with
	    // URLs as a rate different from our consumption, we could be ahead or behind, so we can't
	    // just terminate when there's nothing left to be fetched...more could be on the way.
	    while (!Thread.interrupted()) {
	        FetchList items = null;
	        
	        // Don't bother trying to add more things to the queue if that would only throw
	        // a RejectedExecutionException.
	        if (_pool.getQueue().remainingCapacity() > 0) {
	            items = _provider.poll();
	        }
	        
	        // If we decided to check for IURLs, and we got a set to fetch from one domain...
	        if (items != null) {
	            LOGGER.trace(String.format("Pulled %d items from the %s domain queue", items.size(), items.getDomain()));
	            
	            // Create a Runnable that has a way to fetch the URLs (the IHttpFetcher), and
	            // the list of things to fetch (the <items>).
	            FetcherRunnable command = new FetcherRunnable(_factory.newHttpFetcher(), _collector, items);
	            _pool.execute(command);
	        }
	    }
	    
	    _pool.shutdown();
	    while (!_pool.isShutdown()) {
	        // Spin while waiting.
	        // TODO KKr - have timeout where we call _pool.terminate() to force
	        // it to shut down.
	    }
	} // run
	
	
	/**
	 * Give the caller who set up this manager a way to tell if it's appropatie to
	 * interrupt the fetching process because we're done.
	 * 
	 * @return - true if we're done fetched everything that was in process, and
	 *           there's nothing left to fetch.
	 */
	public boolean isDone() {
	    return (_pool.getActiveCount() == 0) && _provider.isEmpty();
	} // isDone
	
}
