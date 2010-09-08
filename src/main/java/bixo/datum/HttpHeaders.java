package bixo.datum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

import cascading.tuple.Tuple;

public class HttpHeaders implements Writable {
    
    // Patterns used when encoding/decoding header strings
    // These musst match the strings in REPLACEMENT_PATTERNS, and vice versa.
    private static final Pattern CHARS_TO_ENCODE_PATTERN = Pattern.compile("[,\t\n\r\\\\]");
    private static final Pattern CHARS_TO_DECODE_PATTERN = Pattern.compile("(\\,|\\t|\\n|\\r|\\\\)");
    
    private static final String VALUES_DELIMITER = ", ";
    
    private static final String[] REPLACEMENT_PATTERNS = {
        // Convert "\" to "\\"
        "\\\\", "\\\\\\\\",
        
        // Convert , to \,
        ",", "\\\\,",
        
        // Convert <tab> to "\t"
        "\t", "\\\\t",
        
        // Convert <newline> to "\n"
        "\n", "\\\\n",
        
        // Convert <return> to "\r"
        "\r", "\\\\r"
    };
    
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
            List<String> values = getAll(key);
            
            String encodedKey = encodeString(key);
            for (String value : values) {
                if (result.length() > 0) {
                    result.append(VALUES_DELIMITER);
                }

                result.append(encodedKey);
                // TODO KKr - use ":" instead of tab, to avoid problems
                // with parsing output of TextLine cascading tap (uses tabs)
                result.append(": ");
                result.append(encodeString(value));
            }
        }

        return result.toString();
    }
    
    private static String encodeValues(List<String> values) {
        StringBuilder result  = new StringBuilder();
        for (String value : values) {
            if (result.length() > 0) {
                result.append(VALUES_DELIMITER);
            }
            
            result.append(encodeString(value));
        }
        
        return result.toString();
    }

    private static String encodeString(String value) {
        Matcher charMatcher = CHARS_TO_ENCODE_PATTERN.matcher(value);

        // Common case is nothing to replace, so don't worry about performance if we do have anything
        // that we need to convert.
        if (charMatcher.find()) {
            for (int i = 0; i < REPLACEMENT_PATTERNS.length; i += 2) {
                value = value.replaceAll(REPLACEMENT_PATTERNS[i], REPLACEMENT_PATTERNS[i + 1]);
            }
        }

        return value;
    }
    
    private static List<String> decodeValues(String valuesString) {
        List<String> result = new ArrayList<String>();
        
        String[] values = valuesString.split(VALUES_DELIMITER);
        for (String value : values) {
            result.add(decodeString(value));
        }
        
        return result;
    }
    
    private static String decodeString(String value) {
        Matcher charMatcher = CHARS_TO_DECODE_PATTERN.matcher(value);
        if (charMatcher.find()) {
            for (int i = 0; i < REPLACEMENT_PATTERNS.length; i += 2) {
                value = value.replaceAll(REPLACEMENT_PATTERNS[i + 1], REPLACEMENT_PATTERNS[i]);
            }
        }

        return value;
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
