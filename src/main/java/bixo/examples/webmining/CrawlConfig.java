/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

public class CrawlConfig {

    public static final String CRAWLDB_SUBDIR_NAME = "crawldb";
    public static final String CONTENT_SUBDIR_NAME = "content";
    public static final String STATUS_SUBDIR_NAME = "status";
    public static final String RESULTS_SUBDIR_NAME = "results";

    public static final String SPIDER_NAME = "strata-web-miner";
    public static final String WEB_ADDRESS = "http://www.scaleunlimited.com/crawler/strata-web-miner-spider/";
    public static final String EMAIL_ADDRESS = "crawler@scaleunlimited.com";

    public static final String SEED_URLS_FILENAME = "/seed-urls.txt";
    
    // Fetcher policy constants
    public static final int DEFAULT_NUM_THREADS_LOCAL = 2;
    public static final int DEFAULT_NUM_THREADS_CLUSTER = 100;
    public static final int CRAWL_STACKSIZE_KB = 128;
    public static final int MAX_CONTENT_SIZE = 128 * 1024;
    public static final long DEFAULT_CRAWL_DELAY = 10 * 1000L; // in millisecond
    
    // Fetcher constants
    public static final int MAX_RETRIES = 2;
    public static final int SOCKET_TIMEOUT = 10 * 1000;
    public static final int CONNECTION_TIMEOUT = 10 * 1000;

    // Misc
    public static final long MILLISECONDS_PER_MINUTE = 60 * 1000L;
}
