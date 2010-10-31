package bixo.config;

import bixo.config.FetcherPolicy;
import bixo.fetcher.FetchRequest;

// TODO Re-implement this as a FetchJobPolicy, which is where the support should be
@SuppressWarnings("serial")
public class FakeUserFetcherPolicy extends FetcherPolicy {
    
    // Set requests to be about 60 seconds apart (on average)
    private static final long DEFAULT_USER_CRAWL_DELAY = 60 * 1000L;
    
    public FakeUserFetcherPolicy() {
        this(DEFAULT_USER_CRAWL_DELAY);
    }
        
    public FakeUserFetcherPolicy(long crawlDelay) {
        super();
        
        setCrawlDelay(crawlDelay);
    }
    
    @Override
    public long getDefaultCrawlDelay() {
        return DEFAULT_USER_CRAWL_DELAY;
    }
    
    @Override
    public long getCrawlDelay() {
        double baseDelay = super.getCrawlDelay();
        double delayVariance = (Math.random() * baseDelay) - (baseDelay/2.0);
        return Math.round(baseDelay + delayVariance);
    }
    
    public FetchRequest getFetchRequest(long now, long crawlDelay, int maxUrls) {
        // Ignore crawlDelay, and always use our delay.
        long nextRequestTime = now + getCrawlDelay();
        FetchRequest result = new FetchRequest(Math.min(maxUrls, 1), nextRequestTime);
        return result;
    }

}
