package bixo.fetcher;

public enum FetcherCounters {
    ADDED_DOMAIN_QUEUE,     // Added a new domain queue
    
    LISTS_FETCHING,
    
    URLS_QUEUED,
    URLS_SKIPPED,
    
    URLS_FETCHING,
    URLS_FETCHED,
    URLS_ABORTED,
    URLS_FAILED,
    
    FETCHED_BYTES,          // Total bytes of fetched content.
    FETCHED_TIME            // Total time in milliseconds spent fetching
    
}
