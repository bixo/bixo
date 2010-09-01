package bixo.config;

import bixo.fetcher.FetchRequest;

@SuppressWarnings("serial")
public class AdaptiveFetcherPolicy extends FetcherPolicy {
    private static final int MAX_REQUESTS_PER_CONNECTION = 100;
    
    public AdaptiveFetcherPolicy(long crawlEndTime, long crawlDelay) {
        super(DEFAULT_MIN_RESPONSE_RATE, DEFAULT_MAX_CONTENT_SIZE, crawlEndTime, crawlDelay, DEFAULT_MAX_REDIRECTS);
        
        if (crawlEndTime == FetcherPolicy.NO_CRAWL_END_TIME) {
            throw new IllegalArgumentException("crawlEndTime must be set");
        }
    }
    
    public AdaptiveFetcherPolicy(int minResponseRate, int maxContentSize, long crawlEndTime, long crawlDelay) {
        super(minResponseRate, maxContentSize, crawlEndTime, crawlDelay, DEFAULT_MAX_REDIRECTS);
    }
    
    @Override
    public int getMaxRequestsPerConnection() {
        return MAX_REQUESTS_PER_CONNECTION;
    }
    
    @Override
    public FetchRequest getFetchRequest(long now, long crawlDelay, int maxUrls) {
        // we want to fetch maxUrls in the remaining time, but the min delay might constrain us.
        
        if ((getCrawlDelay() == 0) || (maxUrls == 0)) {
            return new FetchRequest(Math.min(maxUrls, getMaxRequestsPerConnection()), now);
        }

        // Even if we're at the end of the crawl, we still want to do our calculation using our
        // default fetch interval, to avoid not crawling anything if we run over. We rely on
        // an external mechanism to do any pruning of remaining URLs, so that they get properly
        // aborted.
        long fetchInterval = Math.max(DEFAULT_FETCH_INTERVAL, getCrawlEndTime() - now);
        
        // Crawl delay must be between the min crawl delay and the default crawl delay.
        long customCrawlDelay = Math.max(getCrawlDelay(), Math.min(DEFAULT_CRAWL_DELAY, fetchInterval / maxUrls));
        
        // Figure out how many URLs we can get in 5 minutes,or the remaining time (whatever is less).
        int numUrls = Math.min((int)(Math.min(DEFAULT_FETCH_INTERVAL, fetchInterval) / customCrawlDelay), maxUrls);
        long nextFetchTime = now + (numUrls * customCrawlDelay);
        return new FetchRequest(numUrls, nextFetchTime);
    }

}
