package bixo.config;

import bixo.fetcher.FetchRequest;

@SuppressWarnings("serial")
public class AdaptiveFetcherPolicy extends FetcherPolicy {
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
    public int getMaxUrls() {
        return calcMaxUrls();
    }
    
    @Override
    public FetchRequest getFetchRequest(int maxUrls) {
        // we want to fetch maxUrls in the remaining time, but the min delay might constrain us.
        
        if ((getCrawlDelay() == 0) || (maxUrls == 0)) {
            return new FetchRequest(maxUrls, System.currentTimeMillis());
        }

        // Even if we're at the end of the crawl, we still want to do our calculation using our
        // default fetch interval, to avoid not crawling anything if we run over. We rely on
        // an external mechanism to do any pruning of remaining URLs, so that they get properly
        // aborted.
        long fetchInterval = Math.max(DEFAULT_FETCH_INTERVAL, getCrawlEndTime() - System.currentTimeMillis());
        
        // Crawl delay must be between _minCrawlDelay and default crawl delay.
        long crawlDelay = Math.max(getCrawlDelay(), Math.min(DEFAULT_CRAWL_DELAY, fetchInterval / maxUrls));
        
        // Figure out how many URLs we can get in 5 minutes,or the remaining time (whatever is less).
        int numUrls = Math.min(1 + (int)(Math.min(DEFAULT_FETCH_INTERVAL, fetchInterval) / crawlDelay), maxUrls);
        
        long nextFetchTime = System.currentTimeMillis() + ((numUrls - 1) * crawlDelay);
        return new FetchRequest(numUrls, nextFetchTime);
    }

}
