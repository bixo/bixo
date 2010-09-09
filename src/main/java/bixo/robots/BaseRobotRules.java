package bixo.robots;

/**
 * Result from parsing a single robots.txt file - which means we
 * get a set of rules, and a crawl-delay.
 */

public abstract class BaseRobotRules {
    public static final long DEFAULT_CRAWL_DELAY = 30 * 1000L;
    public static final long UNSET_CRAWL_DELAY = Long.MIN_VALUE;

    public abstract boolean isAllowed(String url);
    public abstract boolean isAllowAll();
    public abstract boolean isAllowNone();
    
    private long _crawlDelay = UNSET_CRAWL_DELAY;
    private boolean _deferVisits = false;

    public long getCrawlDelay() {
        return _crawlDelay;
    }

    public void setCrawlDelay(long crawlDelay) {
        _crawlDelay = crawlDelay;
    }

    public boolean isDeferVisits() {
        return _deferVisits;
    }
    
    public void setDeferVisits(boolean deferVisits) {
        _deferVisits = deferVisits;
    }
    
}
