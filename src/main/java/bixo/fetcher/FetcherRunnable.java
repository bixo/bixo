package bixo.fetcher;

import org.apache.log4j.Logger;

import bixo.items.FetchItem;

public class FetcherRunnable implements Runnable {
    private static Logger LOGGER = Logger.getLogger(FetcherRunnable.class);
    
    private IHttpFetcher _httpFetcher;
    private FetchCollector _collector;
    private FetchList _items;

    public FetcherRunnable(IHttpFetcher httpFetcher, FetchCollector collector, FetchList items) {
        _httpFetcher = httpFetcher;
        _collector = collector;
        _items = items;
    }
    
    @Override
    public void run() {
        
        for (FetchItem item : _items) {
            try {
                FetchResult result = _httpFetcher.get(item.getUrl());
                LOGGER.trace("Fetched " + result);

                _collector.collect(result);
            } catch (Throwable t) {
                LOGGER.error("Exception: " + t.getMessage(), t);
            }
        }
        
        // All done fetching these items, so we're no longer hitting this domain.
        _items.finished();
    }

}
