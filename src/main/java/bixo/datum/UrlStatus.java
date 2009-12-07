package bixo.datum;

public enum UrlStatus {
    UNFETCHED,                  // Never processed.
    
    // Not fetched due to pre-fetch operations
    SKIPPED_BLOCKED,            // Blocked by robots.txt
    SKIPPED_UNKNOWN_HOST,       // Hostname couldn't be resolved to IP address
    SKIPPED_INVALID_URL,        // URL invalid
    SKIPPED_DEFERRED,           // Deferred because robots.txt couldn't be processed.
    SKIPPED_BY_SCORER,          // Skipped explicitly by scorer or grouper
    SKIPPED_BY_SCORE,           // Skipped because score wasn't high enough
    SKIPPED_TIME_LIMIT,         // Ran out of time
    SKIPPED_FILTERED,           // Filtered out during processing
    
    // Not fetched due to mid-fetch abort
    ABORTED_SLOW_RESPONSE,
    ABORTED_INVALID_MIMETYPE,
    
    // Not fetched during fetch operation, due to HTTP status code error
    HTTP_REDIRECTION_ERROR,
    HTTP_TOO_MANY_REDIRECTS,
    HTTP_MOVED_PERMANENTLY,
    
    HTTP_CLIENT_ERROR,
    HTTP_UNAUTHORIZED,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    HTTP_GONE,
        
    HTTP_SERVER_ERROR,

    // Not fetched during fetch operation, due to error
    ERROR_INVALID_URL,
    ERROR_IOEXCEPTION,
    
    FETCHED;    // Successfully fetched

}
