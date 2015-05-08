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
package bixo.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IoUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoUtils.class);
    
    private IoUtils() {
        // Enforce class isn't instantiated
    }

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
