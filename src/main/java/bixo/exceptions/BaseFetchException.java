package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings("unchecked")
public abstract class BaseFetchException extends Exception {
    @SuppressWarnings({ "serial", "unchecked" })
    private static class NoFetchException extends BaseFetchException implements WritableComparable<NoFetchException> {
        public NoFetchException() {
            super();
        }
        
        public UrlStatus mapToUrlStatus() {
            return UrlStatus.FETCHED;
        }
        
        @Override
        public void readFields(DataInput input) throws IOException {
            readBaseFields(input);
        }

        @Override
        public void write(DataOutput output) throws IOException {
            writeBaseFields(output);
        }

        @Override
        public int compareTo(NoFetchException e) {
            return compareToBase(e);
        }

    }

    // Special value used when processing tuples in the FetchPipe, for fetches that worked.
    // We have to pass something back in the tuple for the exception, as otherwise Cascading
    // will complain, so use this special exception to indicate "no exception" - ughh.
    public static final BaseFetchException NO_FETCH_EXCEPTION = new NoFetchException();
    
    public boolean isRealFetchExcception() {
        return !(this instanceof NoFetchException);
    }
    
    private String _url;
    
    protected BaseFetchException() {
        super();
        _url = null;
    }
    
    protected BaseFetchException(String url) {
        super();
        _url = url;
    }
    
    protected BaseFetchException(String url, String msg) {
        super(msg);
        _url = url;
    }
    
    protected BaseFetchException(String url, Exception e) {
        super(e);
        _url = url;
    }
    
    protected BaseFetchException(String url, String msg, Exception e) {
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
    
    protected int compareToBase(BaseFetchException e) {
        return _url.compareTo(e._url);
    }

    public abstract UrlStatus mapToUrlStatus();
}
