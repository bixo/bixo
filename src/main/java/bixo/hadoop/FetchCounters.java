package bixo.hadoop;

// Counter enums used with Hadoop

public enum FetchCounters {
    DOMAINS_QUEUED,     // Added a new domain queue
    DOMAINS_FINISHED,   // Domains where we've fetched all of the URLs
    DOMAINS_REMAINING,  // Domains left to process
    
    DOMAINS_FETCHING,   // Domains that are in the process of being fetched.
    
    URLS_QUEUED,
    URLS_SKIPPED,
    URLS_REMAINING,     // Number of URLs left to process
    
    URLS_FETCHING,
    URLS_FETCHED,
    URLS_FAILED,
    
    FETCHED_BYTES,          // Total bytes of fetched content.
    FETCHED_TIME            // Total time in milliseconds spent fetching
}
