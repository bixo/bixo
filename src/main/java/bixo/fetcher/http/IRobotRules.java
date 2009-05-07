package bixo.fetcher.http;


public interface IRobotRules {
    public static final int NO_CRAWL_DELAY = -1;
    
    /**
     * Get Crawl-Delay, in milliseconds. This returns NO_CRAWL_DELAY if not set.
     */
    public int getCrawlDelay();

    /**
     * Returns <code>false</code> if the <code>robots.txt</code> file
     * prohibits us from accessing the given <code>url</code>, or
     * <code>true</code> otherwise.
     */
    public boolean isAllowed(String url);

}
