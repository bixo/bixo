package bixo.url;

import java.io.Serializable;

public interface IUrlNormalizer extends Serializable {

    /**
     * Convert <url> into a normalized format, where unimportant differences between
     * two URLs have been removed.
     * 
     * @param url - URL to normalize. Might not be valid, e.g. missing a protocol
     * @return - normalized URL. Still might not be valid, if input URL (for example)
     *           uses an unknown protocol and thus no checks can be done.
     */
    public String normalize(String url);
}
