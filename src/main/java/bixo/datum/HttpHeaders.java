package bixo.datum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class HttpHeaders {
    private static final Logger LOGGER = Logger.getLogger(HttpHeaders.class);
    
    // Patterns used when encoding/decoding header strings
    // These musst match the strings in REPLACEMENT_PATTERNS, and vice versa.
    private static final Pattern CHARS_TO_ENCODE_PATTERN = Pattern.compile("[\t\n\r\f\\\\]");
    private static final Pattern CHARS_TO_DECODE_PATTERN = Pattern.compile("(\\t|\\n|\\r|\\f|\\\\)");
    
    private static final String[] REPLACEMENT_PATTERNS = {
        // Convert "\" to "\\"
        "\\\\", "\\\\\\\\",
        
        // Convert <tab> to "\t"
        "\t", "\\\\t",
        
        // Convert <newline> to "\n"
        "\n", "\\\\n",
        
        // Convert <return> to "\r"
        "\r", "\\\\r",
        
        // Convert (formfeed> to "\f"
        "\f", "\\\\f"
    };
    
    private Map<String, List<String>> _headers;
    
    public HttpHeaders() {
        this(null);
    }
    
    public HttpHeaders(String encodedAsString) {
        _headers = new HashMap<String, List<String>>();
        
        if ((encodedAsString == null) || (encodedAsString.length() == 0)) {
            return;
        }
        
        String[] headerLines = encodedAsString.split("\f");
        for (String headerLine : headerLines) {
            String[] linePieces = headerLine.split("\t");
            if (linePieces.length != 2) {
                LOGGER.error("Invalid encoded header: " + encodedAsString);
            } else {
                add(decodeHeaderString(linePieces[0]), decodeHeaderString(linePieces[1]));
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
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        
        for (String key : getNames()) {
            List<String> values = getAll(key);
            
            String encodedKey = encodeHeaderString(key);
            for (String value : values) {
                result.append(encodedKey);
                result.append('\t');
                result.append(encodeHeaderString(value));
                result.append('\f');
            }
        }
        
        // Get rid of trailing extra return.
        result.setLength(result.length() - 1);
        return result.toString();
    }
    
    private static String encodeHeaderString(String headerString) {
        Matcher charMatcher = CHARS_TO_ENCODE_PATTERN.matcher(headerString);
        
        // Common case is nothing to replace, so don't worry about performance if we do have anything
        // that we need to convert.
        if (charMatcher.find()) {
            String result = headerString;
            for (int i = 0; i < REPLACEMENT_PATTERNS.length; i += 2) {
                result = result.replaceAll(REPLACEMENT_PATTERNS[i], REPLACEMENT_PATTERNS[i + 1]);
            }
            
            return result;
        } else {
            return headerString;
        }
    }

    private static String decodeHeaderString(String headerString) {
        Matcher charMatcher = CHARS_TO_DECODE_PATTERN.matcher(headerString);
        if (charMatcher.find()) {
            String result = headerString;
            for (int i = 0; i < REPLACEMENT_PATTERNS.length; i += 2) {
                result = result.replaceAll(REPLACEMENT_PATTERNS[i + 1], REPLACEMENT_PATTERNS[i]);
            }
            
            return result;
        } else {
            return headerString;
        }
    }

}
