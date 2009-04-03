package bixo.fetcher;

public class FetcherPolicy {
    public static final int NO_CRAWL_DELAY = 0;
    public static final int DEFAULT_CRAWL_DELAY = 30;
    public static final int DEFAULT_THREADS_PER_HOST = 1;
    public static final int DEFAULT_REQUESTS_PER_CONNECTION = 1;

    private int _crawlDelay;			// Delay between requests, in seconds.
    private int _threadsPerHost;		// > 1 => ignore crawl delay
    private int _requestsPerConnection;	// > 1 => using keep-alive.
    // TODO KKr - add RobotExclusion instance here

    public FetcherPolicy() {
        this(DEFAULT_CRAWL_DELAY, DEFAULT_THREADS_PER_HOST, DEFAULT_REQUESTS_PER_CONNECTION);
    }


    public FetcherPolicy(int crawlDelay, int threadsPerHost, int requestsPerConnection) {
        _crawlDelay = crawlDelay;
        _threadsPerHost = threadsPerHost;
        _requestsPerConnection = requestsPerConnection;
    }


    public int getCrawlDelay() {
        if (_threadsPerHost > 1) {
            return 0;
        } else {
            return _crawlDelay;
        }
    }


    public void setCrawlDelay(int crawlDelay) {
        _crawlDelay = crawlDelay;
    }


    public int getThreadsPerHost() {
        return _threadsPerHost;
    }


    public void setThreadsPerHost(int threadsPerHost) {
        _threadsPerHost = threadsPerHost;
    }


    public int getRequestsPerConnection() {
        return _requestsPerConnection;
    }


    public void setRequestsPerConnect(int requestsPerConnection) {
        _requestsPerConnection = requestsPerConnection;
    }

}
