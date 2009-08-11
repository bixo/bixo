package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class IOFetchException extends BixoFetchException implements WritableComparable<IOFetchException> {
    
    public IOFetchException() {
        super();
    }
    
    public IOFetchException(String url, IOException e) {
        super(url, e);
    }

    @Override
    public UrlStatus mapToUrlStatus() {
        return UrlStatus.ERROR_IOEXCEPTION;
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
    public int compareTo(IOFetchException e) {
        return compareToBase(e);
    }

}
