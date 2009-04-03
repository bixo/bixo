package bixo.fetcher;

import org.apache.log4j.Logger;

public class FetcherRunnable implements Runnable {
    private static Logger LOGGER = Logger.getLogger(FetcherRunnable.class);
    
    private IHttpFetcher _httpFetcher;
    private FetchList _items;

    public FetcherRunnable(IHttpFetcher httpFetcher, FetchList items) {
        _httpFetcher = httpFetcher;
        _items = items;
    }
    
    @Override
    public void run() {
        
        for (FetchItem item : _items) {
            try {
                FetchResult result = _httpFetcher.get(item.getUrl().toExternalForm());
                LOGGER.trace("Fetched " + result);

                // TODO KKr - what to do with result?
            } catch (Throwable t) {
                LOGGER.error("Exception: " + t.getMessage(), t);
            }
        }
        
        // All done fetching these items, so we're no longer hitting this domain.
        _items.finished();
    }

}
