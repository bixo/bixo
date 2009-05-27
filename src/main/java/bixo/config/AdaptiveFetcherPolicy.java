package bixo.config;

import bixo.fetcher.FetchRequest;

@SuppressWarnings("serial")
public class AdaptiveFetcherPolicy extends FetcherPolicy {
    private int _minCrawlDelay;
    
    public AdaptiveFetcherPolicy(long crawlEndTime, int minCrawlDelay) {
        super(DEFAULT_MIN_RESPONSE_RATE, DEFAULT_MAX_CONTENT_SIZE, crawlEndTime);
        
        if (crawlEndTime == FetcherPolicy.NO_CRAWL_END_TIME) {
            throw new IllegalArgumentException("crawlEndTime must be set");
        }

        _minCrawlDelay = minCrawlDelay;
    }
    
    public AdaptiveFetcherPolicy(int minResponseRate, int maxContentSize, long crawlEndTime, int minCrawlDelay) {
        super(minResponseRate, maxContentSize, crawlEndTime);
        _minCrawlDelay = minCrawlDelay;
    }
    
    public void setMinCrawlDelay(int minCrawlDelay) {
        _minCrawlDelay = minCrawlDelay;
    }
    
    @Override
    public int getMaxUrls() {
        return calcMaxUrls(_minCrawlDelay);
    }
    
    @Override
    public FetchRequest getFetchRequest(int maxUrls) {
        // we want to fetch maxUrls in the remaining time, but the min delay might constrain us.
        
        if ((_minCrawlDelay == 0) || (maxUrls == 0)) {
            return new FetchRequest(maxUrls, System.currentTimeMillis());
        }

        // Even if we're at the end of the crawl, we still want to do our calculation using our
        // default fetch interval, to avoid not crawling anything if we run over. We rely on
        // an external mechanism to do any pruning of remaining URLs, so that they get properly
        // aborted.
        long fetchInterval = Math.max(DEFAULT_FETCH_INTERVAL * 1000L, getCrawlEndTime() - System.currentTimeMillis());
        
        // Crawl delay must be between _minCrawlDelay and default crawl delay.
        int intervalInSeconds = (int)(fetchInterval / 1000L);
        int crawlDelay = Math.max(_minCrawlDelay, Math.min(DEFAULT_CRAWL_DELAY, intervalInSeconds / maxUrls));
        
        // Figure out how many URLs we can get in 5 minutes,or the remaining time (whatever is less).
        int numUrls = Math.min(1 + (Math.min(DEFAULT_FETCH_INTERVAL, intervalInSeconds) / crawlDelay), maxUrls);
        
        long nextFetchTime = System.currentTimeMillis() + ((numUrls - 1) * crawlDelay * 1000L);
        return new FetchRequest(numUrls, nextFetchTime);
    }

}
