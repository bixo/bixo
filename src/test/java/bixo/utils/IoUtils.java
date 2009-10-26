package bixo.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

public class IoUtils {
    private static final Logger LOGGER = Logger.getLogger(IoUtils.class);
    
    public static void safeClose(InputStream is) {
        if (is == null) {
            return;
        }
        
        try {
            is.close();
        } catch (IOException e) {
            LOGGER.warn("IOException closing input stream", e);
        }
    }
}
