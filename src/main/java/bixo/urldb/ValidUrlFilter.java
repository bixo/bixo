package bixo.urldb;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Simple UrlFilter that just makes sure the URL is considered to be
 * "valid" by the rather simple rules encoded in the java.net URL class.
 */
public class ValidUrlFilter implements IUrlFilter {

    @Override
    public String filter(String line) {
        try {
            URL url = new URL(line);
            return url.toExternalForm();
        } catch (MalformedURLException e) {
            return null;
        }
    }

}
