package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class UrlFetchException extends FetchException implements WritableComparable<UrlFetchException> {
    
    public UrlFetchException() {
        super();
    }
    
    public UrlFetchException(String url, String msg) {
        super(url, msg);
    }

    @Override
    public UrlStatus mapToUrlStatus() {
        return UrlStatus.ERROR_INVALID_URL;
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
    public int compareTo(UrlFetchException e) {
        return compareToBase(e);
    }

}
