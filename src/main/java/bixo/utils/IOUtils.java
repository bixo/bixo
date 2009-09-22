package bixo.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

public class IOUtils {
    private static final Logger LOGGER = Logger.getLogger(IOUtils.class);
    
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
