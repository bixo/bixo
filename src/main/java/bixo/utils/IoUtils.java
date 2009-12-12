package bixo.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

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
    
    public static void safeClose(OutputStream os) {
        if (os == null) {
            return;
        }
        
        try {
            os.close();
        } catch (IOException e) {
            LOGGER.warn("IOException closing input stream", e);
        }
    }
    
	/**
	 * Read one line of input from the console.
	 * 
	 * @return Text that the user entered
	 * @throws IOException
	 */
	public static String readInputLine() throws IOException {
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);
		return br.readLine();
	}


}
