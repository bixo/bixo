package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class AbortedFetchException extends BixoFetchException implements WritableComparable<AbortedFetchException> {
    private AbortedFetchReason _abortReason;
    
    public AbortedFetchException() {
        super();
    }
    
    public AbortedFetchException(String url, AbortedFetchReason abortReason) {
        super(url);
        
        _abortReason = abortReason;
    }
    
    public AbortedFetchReason getAbortReason() {
        return _abortReason;
    }

    @Override
    public UrlStatus mapToUrlStatus() {
        switch (_abortReason) {
        case USER_REQUESTED:
            return UrlStatus.ABORTED_USER_REQUEST;

        case SLOW_RESPONSE_RATE:
            return UrlStatus.ABORTED_SLOW_RESPONSE;

        case TIME_LIMIT:
            return UrlStatus.ABORTED_TIME_LIMIT;

        default:
            throw new RuntimeException("Unknown abort reason: " + _abortReason);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        readBaseFields(input);
        
        _abortReason = AbortedFetchReason.valueOf(input.readUTF());
    }

    @Override
    public void write(DataOutput output) throws IOException {
        writeBaseFields(output);
        output.writeUTF(_abortReason.name());
    }

    @Override
    public int compareTo(AbortedFetchException e) {
        int result = compareToBase(e);
        if (result == 0) {
            result = _abortReason.compareTo(e._abortReason);
        }
        
        return result;
    }
}
