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
package bixo.datum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import cascading.tuple.Tuple;

public class HttpHeaders implements Writable {
    
    private Map<String, List<String>> _headers;
    
    public HttpHeaders() {
        this(null);
    }
    
    public HttpHeaders(Tuple headers) {
        _headers = new HashMap<String, List<String>>();
        
        if (headers != null) {
            int numEntries = headers.size() / 2;
            
            for (int i = 0; i < numEntries; i++) {
                String name = decodeString(headers.getString(i * 2));
                List<String> values = decodeValues(headers.getString((i * 2) + 1));
                _headers.put(name, values);
            }
        }
    }
    
    public void add(String name, String value) {
        String normalizedName = normalize(name);
        List<String> curValues = _headers.get(normalizedName);
        if (curValues == null) {
            curValues = new ArrayList<String>();
            _headers.put(normalizedName, curValues);
        }
        
        curValues.add(value);
    }
    
    public String getFirst(String name) {
        String normalizedName = normalize(name);
        List<String> curValues = _headers.get(normalizedName);
        if (curValues == null) {
            return null;
        } else {
            return curValues.get(0);
        }
    }
    
    public List<String> getAll(String name) {
        String normalizedName = normalize(name);
        List<String> curValues = _headers.get(normalizedName);
        if (curValues == null) {
            return new ArrayList<String>();
        } else {
            return curValues;
        }
    }
    
    public Set<String> getNames() {
        return _headers.keySet();
    }
    
    private static String normalize(String name) {
        return name.toLowerCase();
    }
    
    public Tuple toTuple() {
        Tuple result = new Tuple();
        for (String name : _headers.keySet()) {
            List<String> values = _headers.get(name);
            result.add(encodeString(name));
            result.add(encodeValues(values));
        }
        
        return result;
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        
        for (String key : getNames()) {
            if (result.length() > 0) {
                result.append("; ");
            }

            List<String> values = getAll(key);
            result.append(encodeString(key));
            result.append("=");

            int numValues = 0;
            for (String value : values) {
                if (numValues > 0) {
                    result.append(",");
                }

                result.append(encodeString(value));
                numValues += 1;
            }
        }

        return result.toString();
    }
    
    private static String encodeValues(List<String> values) {
        StringBuilder result  = new StringBuilder();
        for (String value : values) {
            if (result.length() > 0) {
                result.append(',');
            }
            
            result.append(encodeString(value));
        }
        
        return result.toString();
    }

    private static String encodeString(String value) {
        // We know that tabs and commas (our special chars) will be encoded
        // by URLEncoder.
        
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }
    
    private static List<String> decodeValues(String valuesString) {
        List<String> result = new ArrayList<String>();
        
        String[] values = valuesString.split(",");
        for (String value : values) {
            result.add(decodeString(value));
        }
        
        return result;
    }
    
    private static String decodeString(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int numHeaders = in.readInt();
        _headers = new HashMap<String, List<String>>(numHeaders);
        
        for (int i = 0; i < numHeaders; i++) {
            String name = in.readUTF();
            int numValues = in.readInt();
            List<String> values = new ArrayList<String>(numValues);
            for (int j = 0; j < numValues; j++) {
                values.add(in.readUTF());
            }
            
            _headers.put(name, values);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(_headers.size());
        for (String name : _headers.keySet()) {
            List<String> values = _headers.get(name);
            out.writeUTF(name);
            out.writeInt(values.size());
            for (String value : values) {
                out.writeUTF(value);
            }
        }
    }

}
