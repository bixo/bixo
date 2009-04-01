package bixo;

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

    private static Logger LOG = Logger.getLogger(HadoopConfigured.class);

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
            LOG.warn("The path: " + path + " is not a valid uri. Therefore we use the configured file system.", e);
        }
        return getFileSystem();
    }
}
