package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial", "unchecked" })
public class NoFetchException extends BixoFetchException implements WritableComparable<NoFetchException> {
    public NoFetchException() {
        super();
    }
    
    public NoFetchException(String url) {
        super(url);
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
