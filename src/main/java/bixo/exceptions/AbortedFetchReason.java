package bixo.exceptions;

public enum AbortedFetchReason {
    SLOW_RESPONSE_RATE,
    
    INVALID_MIME_TYPE;      // FetcherPolicy doesn't specify this as a valid mime-type
}
