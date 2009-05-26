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
        
        long remainingTime = getCrawlEndTime() - System.currentTimeMillis();
        if (remainingTime <= 0) {
            return new FetchRequest(0, System.currentTimeMillis());
        }
        
        // Crawl delay must be between _minCrawlDelay and default crawl delay.
        int remainingSeconds = (int)(remainingTime / 1000L);
        int crawlDelay = Math.max(_minCrawlDelay, Math.min(DEFAULT_CRAWL_DELAY, remainingSeconds / maxUrls));
        
        // Figure out how many URLs we can get in 5 minutes,or the remaining time (whatever is less).
        int numUrls = Math.min(1 + (Math.min(5 * 60, remainingSeconds) / crawlDelay), maxUrls);
        
        // Our next fetch time is either at the end of the fetch cycle (if we couldn't get all of the URLs
        // due to our min crawl delay) or the calculated value.
        long nextFetchTime = Math.min(getCrawlEndTime(), System.currentTimeMillis() + ((numUrls - 1) * crawlDelay * 1000L));
        return new FetchRequest(numUrls, nextFetchTime);
    }

}
