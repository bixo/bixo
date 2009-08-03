package bixo.config;

import bixo.config.FetcherPolicy;
import bixo.fetcher.FetchRequest;

@SuppressWarnings("serial")
public class FakeUserFetcherPolicy extends FetcherPolicy {
    
    // Set requests to be about 60 seconds apart (on average)
    private static final int DEFAULT_USER_CRAWL_DELAY = 60;
    
    private int _crawlDelay = DEFAULT_USER_CRAWL_DELAY;
    
    public FakeUserFetcherPolicy() {
    }
        
    public FakeUserFetcherPolicy(int crawlDelay) {
        _crawlDelay = crawlDelay;
    }
        
    @Override
    public FetchRequest getFetchRequest(int maxUrls) {
        // Set up the next request to have random variance.
        double baseDelay = _crawlDelay * 1000.0;
        double delayVariance = (Math.random() * baseDelay) - (baseDelay/2.0);
        long nextRequestTime = System.currentTimeMillis() + Math.round(baseDelay + delayVariance);
        FetchRequest result = new FetchRequest(Math.min(maxUrls, 1), nextRequestTime);
        return result;
    }

    @Override
    public int getDefaultCrawlDelay() {
        return _crawlDelay;
    }
}
