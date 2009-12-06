package bixo.utils;

import java.security.InvalidParameterException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpUtils {

    private static final Pattern CONTENT_TYPE_PATTERN = Pattern.compile("(?i)\\s*([^; ]*)\\s*(?:|;\\s*charset\\s*=\\s*([^ ]*)\\s*)");
    
    public static String getMimeTypeFromContentType(String contentType) {
        Matcher m = CONTENT_TYPE_PATTERN.matcher(contentType);
        if (!m.matches()) {
            throw new InvalidParameterException("Format of content-type not valid: " + contentType);
        }
        
        return m.group(1);
    }
    
    public static String getCharsetFromContentType(String contentType) {
        Matcher m = CONTENT_TYPE_PATTERN.matcher(contentType);
        if (!m.matches()) {
            throw new InvalidParameterException("Format of content-type not valid: " + contentType);
        }
        
        String result = m.group(2);
        return (result == null ? "" : result);
    }
}
