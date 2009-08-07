package bixo.exceptions;

@SuppressWarnings("serial")
public class BixoFetchException extends Exception {
    private final int _httpStatus;
    
    public BixoFetchException(int httpStatus, String msg) {
        super(msg);
        
        _httpStatus = httpStatus;
    }
    
    public BixoFetchException(int httpStatus, String msg, Exception e) {
        super(msg, e);
        
        _httpStatus = httpStatus;
    }
    
    public int getHttpStatus() {
        return _httpStatus;
    }
}
