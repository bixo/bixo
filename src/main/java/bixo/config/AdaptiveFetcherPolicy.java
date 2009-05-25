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
        if (_minCrawlDelay == 0) {
            return new FetchRequest(maxUrls, System.currentTimeMillis());
        }
        
        int numUrls = Math.min(maxUrls, calcMaxUrls(_minCrawlDelay));
        
        // Our next fetch time is either at the end of the fetch cycle (if we had to adaptively cut down the crawl delay), 
        // or when the normal fetch time would be given the default delay.
        long nextFetchTime = Math.min(getCrawlEndTime(), System.currentTimeMillis() + (numUrls * DEFAULT_CRAWL_DELAY * 1000L));
        return new FetchRequest(numUrls, nextFetchTime);
    }

}
