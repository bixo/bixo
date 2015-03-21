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
public class IOFetchException extends BaseFetchException implements WritableComparable<IOFetchException> {
    
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
