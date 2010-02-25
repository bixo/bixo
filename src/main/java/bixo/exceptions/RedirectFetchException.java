package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class RedirectFetchException extends BaseFetchException implements WritableComparable<RedirectFetchException> {
    
    private String _redirectedUrl;
    
    public RedirectFetchException() {
        super();
    }
    
    public RedirectFetchException(String url, String redirectedUrl) {
        super(url, "Too many redirects");
        _redirectedUrl = redirectedUrl;
    }
    
    public String getRedirectedUrl() {
        return _redirectedUrl;
    }
    
    @Override
    public UrlStatus mapToUrlStatus() {
        return UrlStatus.ERROR_IOEXCEPTION;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        readBaseFields(input);
        
        _redirectedUrl = input.readUTF();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        writeBaseFields(output);
        output.writeUTF(_redirectedUrl);
    }

    @Override
    public int compareTo(RedirectFetchException e) {
        int result = compareToBase(e);
        if (result == 0) {
            result = _redirectedUrl.compareTo(e._redirectedUrl);
        }
        
        return result;
    }

}
