package bixo.utils;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class CharsetUtils {
    private static final Logger LOGGER = Logger.getLogger(CharsetUtils.class);
    
    private static final Pattern CHARSET_NAME_PATTERN = Pattern.compile("[ \\\"]*([^ ;\\\"]+).*");
    
    public static boolean safeIsSupported(String charsetName) {
        try {
            return Charset.isSupported(charsetName);
        } catch (IllegalCharsetNameException e) {
            return false;
        } catch (IllegalArgumentException e) {
            return false;
        } catch (Exception e) {
            LOGGER.warn("Unexpected exception", e);
            return false;
        }
    }
    
    public static String clean(String charsetName) {
        if (charsetName == null) {
            return null;
        }
        
        Matcher m = CHARSET_NAME_PATTERN.matcher(charsetName);
        if (!m.matches()) {
            return null;
        } else {
            return m.group(1);
        }
    }
}
