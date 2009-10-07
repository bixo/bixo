package bixo.fetcher.http;

import java.net.MalformedURLException;
import java.net.URL;


public interface IRobotRules {
    public static final long DEFAULT_CRAWL_DELAY = 30 * 1000L;
    public static final long UNSET_CRAWL_DELAY = Long.MIN_VALUE;
    
    /**
     * Get Crawl-Delay, in milliseconds. This returns UNSET_CRAWL_DELAY if not set.
     */
    public long getCrawlDelay();

    /**
     * Returns <code>false</code> if the <code>robots.txt</code> file
     * prohibits us from accessing the given <code>url</code>, or
     * <code>true</code> otherwise.
     */
    public boolean isAllowed(String url) throws MalformedURLException;

    public boolean isAllowed(URL url);
    
    /**
     * If true, caller should bail on visiting any of the pages until the server
     * error status w.r.t. robots.txt is resolved.
     * 
     * @return true if there was a problem getting/processing the robots.txt file.
     */
    public boolean getDeferVisits();
}
