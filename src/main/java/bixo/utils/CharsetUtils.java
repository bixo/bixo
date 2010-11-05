package bixo.utils;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

// TODO We should be using Tika's CharsetUtils instead.
@SuppressWarnings("serial")
public class CharsetUtils {
    private static final Logger LOGGER = Logger.getLogger(CharsetUtils.class);
    
    private static final Pattern CHARSET_NAME_PATTERN = Pattern.compile("[ \\\"]*([^ >,;\\\"]+).*");
    private static final Pattern ISO_NAME_PATTERN = Pattern.compile("(?i).*8859-([\\d]+)");
    private static final Pattern CP_NAME_PATTERN = Pattern.compile("(?i)cp-([\\d]+)");
    private static final Pattern WIN_NAME_PATTERN = Pattern.compile("(?i)win(|-)([\\d]+)");
    
    private static final Map<String, String> CHARSET_ALIASES = new HashMap<String, String>() {{
        put("none", null);
        put("no", null);
        
        put("iso-8851-1", "iso-8859-1");
        
        put("windows", "windows-1252");
        
        put("koi8r", "KOI8-R");
    }};
    
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
        }

        String result = m.group(1);
        if (CHARSET_ALIASES.containsKey(result.toLowerCase())) {
            result = CHARSET_ALIASES.get(result.toLowerCase());
        } else if (ISO_NAME_PATTERN.matcher(result).matches()) {
            m = ISO_NAME_PATTERN.matcher(result);
            m.matches();
            result = "iso-8859-" + m.group(1);
        } else if (CP_NAME_PATTERN.matcher(result).matches()) {
            m = CP_NAME_PATTERN.matcher(result);
            m.matches();
            result = "cp" + m.group(1);
        } else if (WIN_NAME_PATTERN.matcher(result).matches()) {
            m = WIN_NAME_PATTERN.matcher(result);
            m.matches();
            result = "windows-" + m.group(2);
        }
        
        try {
            Charset cs = Charset.forName(result);
            return cs.name();
        } catch (Exception e) {
            return null;
        }
    }
}
