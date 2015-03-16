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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public abstract class BaseFetchException extends Exception {
    
    private String _url = "";
    private String _msg = "";
    private Throwable _cause = null;
    
	protected BaseFetchException() {
		this("");
    }
    
    protected BaseFetchException(String url) {
    	this(url, "");
    }
    
    protected BaseFetchException(String url, String msg) {
    	this(url, msg, null);
    }
    
    protected BaseFetchException(String url, Exception e) {
    	this(url, "", e);
    }
    
    protected BaseFetchException(String url, String msg, Exception e) {
        super();
        
        // We have to set the cause to null because otherwise the cause
        // is auto-set to be "this", and Kryo will recurse forever trying
        // to serialize it.
    	initCause(null);
    	
    	_cause = e;
    	_msg = msg;
        _url = url;
    }
    
    // Our specific methods
    public String getUrl() {
        return _url;
    }
    
    protected int compareToBase(BaseFetchException e) {
        return _url.compareTo(e._url);
    }

    public abstract UrlStatus mapToUrlStatus();

    @Override
    public String getMessage() {
        return _msg;
    }
    
    @Override
    public String getLocalizedMessage() {
        return getMessage();
    }
    
    @Override
    public Throwable getCause() {
        return _cause;
    }
    
	@Override
	public String toString() {
        String message = getLocalizedMessage();
        return String.format("%s: %s [%s]", getClass().getSimpleName(), message != null ? message : "", _url);
	}

    protected void readBaseFields(DataInput input) throws IOException {
    	int serializedLen = input.readInt();
    	byte[] serialized = new byte[serializedLen];
    	input.readFully(serialized);
    	ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized));
    	
    	try {
    	    _cause = (Throwable)ois.readObject();
    	} catch (ClassNotFoundException e) {
    		throw new IOException(e);
    	}
    	
    	_msg = input.readUTF();
        _url = input.readUTF();
    }
    
    protected void writeBaseFields(DataOutput output) throws IOException {
        // Write out the Throwable cause
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
    	ObjectOutputStream oos = new ObjectOutputStream(bos);
    	oos.writeObject(_cause);
    	byte[] serialized = bos.toByteArray();
    	output.writeInt(serialized.length);
    	output.write(bos.toByteArray());
    	
    	// Write out the message, and our URL
    	output.writeUTF(_msg);
        output.writeUTF(_url);
    }
    
}
