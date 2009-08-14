package bixo.datum;

public enum UrlStatus {
    UNFETCHED,
    FETCHED,
    
    ABORTED_SLOW_RESPONSE,
    ABORTED_TIME_LIMIT,
    ABORTED_USER_REQUEST,
    
    HTTP_REDIRECTION_ERROR,
    HTTP_TOO_MANY_REDIRECTS,
    HTTP_MOVED_PERMANENTLY,
    
    HTTP_CLIENT_ERROR,
    HTTP_UNAUTHORIZED,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    HTTP_GONE,
        
    HTTP_SERVER_ERROR,

    ERROR_INVALID_URL,
    ERROR_IOEXCEPTION;
    
}