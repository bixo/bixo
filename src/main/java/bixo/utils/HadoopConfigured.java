/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * Helps to deal with all the hadoop configuration related lookups.
 * 
 */
public class HadoopConfigured {

    private static Logger LOGGER = Logger.getLogger(HadoopConfigured.class);

    private Configuration _conf;

    public HadoopConfigured() {
        _conf = new Configuration();
    }

    public Configuration getConf() {
        return _conf;
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(getConf());
    }

    public FileSystem getFileSystem(URI uri) throws IOException {
        return FileSystem.get(uri, getConf());
    }

    /**
     * If the path is a valid URI we lookup the file system based on the uri, if
     * it is not we use the configured file system.
     * Please note that "/something" is a valid uri and will return the local file system.
     * 
     * @throws IOException
     */
    public FileSystem getFileSystem(String path) throws IOException {
        try {
            URI uri = new URI(path);
            return FileSystem.get(uri, getConf());
        } catch (URISyntaxException e) {
            LOGGER.warn("The path: " + path + " is not a valid uri. Therefore we use the configured file system.", e);
        }
        return getFileSystem();
    }
}
