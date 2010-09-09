package bixo.examples;

public class CrawlConfig {

    public static final String CRAWLDB_SUBDIR_NAME = "crawldb";
    public static final String CONTENT_SUBDIR_NAME = "content";
    public static final String STATUS_SUBDIR_NAME = "status";
    public static final String PARSE_SUBDIR_NAME = "parse";



    public static final String WEB_ADDRESS = "http://wiki.github.com/bixo/bixo/bixocrawler";
    public static final String EMAIL_ADDRESS = "bixo-dev@yahoogroups.com";

    // Fetcher policy constants
    public static final int CRAWL_STACKSIZE_KB = 128;
    public static final int MAX_CONTENT_SIZE = 128 * 1024;
    public static final long DEFAULT_CRAWL_DELAY = 10 * 1000L; // in millisecond
//    public static final int MAX_CONNECTIONS_PER_HOST = 30;
//    public static final int MAX_REQUESTS_PER_CONNECTION = 10;
//    public static final int MIN_RESPONSE_RATE = 1024;          // Min bytes/second
    
    // Fetcher constants
    public static final int MAX_RETRIES = 2;
    public static final int SOCKET_TIMEOUT = 10 * 1000;
    public static final int CONNECTION_TIMEOUT = 10 * 1000;

    // Misc
    public static final long MILLISECONDS_PER_MINUTE = 60 * 1000L;
}
