/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class AbortedFetchException extends BaseFetchException implements WritableComparable<AbortedFetchException> {
    private AbortedFetchReason _abortReason;
    
    public AbortedFetchException() {
        super();
    }
    
    public AbortedFetchException(String url, AbortedFetchReason abortReason) {
        super(url, "Aborted due to " + abortReason);
        
        _abortReason = abortReason;
    }
    
    public AbortedFetchException(String url, String msg, AbortedFetchReason abortReason) {
        super(url, msg);
        
        _abortReason = abortReason;
    }
    
    public AbortedFetchReason getAbortReason() {
        return _abortReason;
    }

    @Override
    public UrlStatus mapToUrlStatus() {
        switch (_abortReason) {
        case SLOW_RESPONSE_RATE:
            return UrlStatus.ABORTED_SLOW_RESPONSE;

        case INVALID_MIMETYPE:
        case CONTENT_SIZE:
            return UrlStatus.ABORTED_FETCHER_POLICY;
            
        case INTERRUPTED:
            return UrlStatus.SKIPPED_INTERRUPTED;
        
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
