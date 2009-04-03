package bixo.parser;

/**
 * Filter or normalize urls
 * 
 */
public interface UrlFilter {

    /**
     * Return a normalized string or null if the url does not pass the filter
     */
    String filter(String line);

}
