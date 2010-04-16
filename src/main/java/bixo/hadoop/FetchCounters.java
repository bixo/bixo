package bixo.hadoop;

// Counter enums used with Hadoop

public enum FetchCounters {
    DOMAINS_FINISHED,   // Domains where we've processed all of the robots.txt
    DOMAINS_PROCESSING, // Domains that are in the process of being fetched/robots.txt
    
    // Specific to robots.txt processing 
    DOMAINS_REJECTED,   // Domains we rejected
    DOMAINS_SKIPPED,    // Domains we ignored
    DOMAINS_DEFERRED,   // Domains we deferred
    URLS_ACCEPTED,      // URLs we accepted
    URLS_DEFERRED,      // URLs we deferred, because domain was deferred
    URLS_REJECTED,      // URLS we rejected, because domain was rejected
    
    // During URL fetching
    URLS_FETCHING,
    URLS_FETCHED,
    URLS_SKIPPED,
    URLS_FAILED,
    
    FETCHED_BYTES,          // Total bytes of fetched content.
    FETCHED_TIME            // Total time in milliseconds spent fetching

}
