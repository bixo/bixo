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
package bixo.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helps to deal with all the hadoop configuration related lookups.
 * 
 */
public class HadoopConfigured {

    private static Logger LOGGER = LoggerFactory.getLogger(HadoopConfigured.class);

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
