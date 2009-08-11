package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import bixo.datum.UrlStatus;

@SuppressWarnings("unchecked")
public abstract class BixoFetchException extends Exception {
    
    // Special value used when processing tuples in the FetchPipe, for fetches that worked.
    public static final BixoFetchException NO_FETCH_EXCEPTION = new NoFetchException("");
    
    private String _url;
    
    protected BixoFetchException() {
        super();
        _url = null;
    }
    
    protected BixoFetchException(String url) {
        super();
        _url = url;
    }
    
    protected BixoFetchException(String url, String msg) {
        super(msg);
        _url = url;
    }
    
    protected BixoFetchException(String url, Exception e) {
        super(e);
        _url = url;
    }
    
    protected BixoFetchException(String url, String msg, Exception e) {
        super(msg, e);
        _url = url;
    }
    
    public String getUrl() {
        return _url;
    }
    
    protected void readBaseFields(DataInput input) throws IOException {
        _url = input.readUTF();
    }
    
    protected void writeBaseFields(DataOutput output) throws IOException {
        output.writeUTF(_url);
    }
    
    protected int compareToBase(BixoFetchException e) {
        return _url.compareTo(e._url);
    }

    public abstract UrlStatus mapToUrlStatus();
}
