package bixo.fetcher;

public enum FetcherCounters {
    DOMAINS_QUEUED,     // Added a new domain queue
    DOMAINS_FINISHED,   // Domains where we've fetched all of the URLs
    
    DOMAINS_FETCHING,   // Domains that are in the process of being fetched.
    
    URLS_QUEUED,
    URLS_SKIPPED,
    
    URLS_FETCHING,
    URLS_FETCHED,
    URLS_ABORTED,
    URLS_FAILED,
    
    FETCHED_BYTES,          // Total bytes of fetched content.
    FETCHED_TIME            // Total time in milliseconds spent fetching
}
