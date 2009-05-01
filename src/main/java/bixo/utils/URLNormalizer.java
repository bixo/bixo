package bixo.utils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

public class URLNormalizer {
    private static final Logger LOGGER = Logger.getLogger(URLNormalizer.class);
    
    public String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Should never happen, so log it
            LOGGER.error("Unexpected exception during URL encoding", e);
            return url;
        }
    }

    
    public String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Should never happen, so log it
            LOGGER.error("Unexpected exception during URL decoding", e);
            return url;
        }
    }

    
    public String normalizeHostname(String hostname) {
        // Convert domain.com => www.domain.com
        String result = hostname.toLowerCase();
        String paidLevelDomain = DomainNames.getPLD(result);
        if (result.equals(paidLevelDomain)) {
            result = "www." + paidLevelDomain;
        }
        
        return result;
    }
    
    
    // Now we get to re-encode the path and query portions of the URL, but we have
    // to split up the path as otherwise '/' => %2F.
    public String normalizePath(String path) {
        String[] pathParts = path.split("/");
        
        StringBuilder newPath = new StringBuilder();
        for (String pathPart : pathParts) {
            newPath.append(encodeUrl(decodeUrl(pathPart)));
            newPath.append('/');
        }
        
        if (!path.endsWith("/")) {
            newPath.setLength(newPath.length() - 1);
        }
        
        if (newPath.length() == 0) {
            return "/";
        } else {
            return newPath.toString();
        }
    }

    
    // For the query portion, handle decoding and then re-encoding the portions
    // between '&' and then '=' characters.
    public String normalizeQuery(String query) {
        if (query == null) {
            return "";
        }

        StringBuilder newQuery = new StringBuilder();
        String[] queryParts = query.split("&");
        for (String queryPart : queryParts) {
            String[] keyValues = queryPart.split("=");
            if (keyValues.length == 1) {
                newQuery.append(encodeUrl(decodeUrl(keyValues[0])));
                if (queryPart.endsWith("=")) {
                    newQuery.append("=");
                }
            } else {
                for (String kvPart : keyValues) {
                    newQuery.append(encodeUrl(decodeUrl(kvPart)));
                    newQuery.append('=');
                }

                newQuery.setLength(newQuery.length() - 1);
            }

            newQuery.append('&');
        }

        // Remove last '&'
        newQuery.setLength(newQuery.length() - 1);
        newQuery.insert(0, '?');
        return newQuery.toString();
    }

    
    public String normalize(String url) {
        String result = url.trim();
        
        
        // First see if there is any protocol - if not, append http:// by default.
        if (result.indexOf("://") == -1) {
            // FUTURE - could put some limit on max length of protocol string.
            result = "http://" + result;
        }
        
        URL testUrl;
        
        try {
            String decodedUrl = url.replace("+", "%20");
            testUrl = new URL(decodedUrl);
        } catch (MalformedURLException e) {
            // Not a valid URL we know about, so in this case we're just going to
            // return it as-is, other than the stripping we did.
            return result;
        }
        
        // Don't do additional special processing for anything other than http/https protocols.
        String protocol = testUrl.getProtocol().toLowerCase();
        if (!protocol.equals("http") && !protocol.equals("https")) {
            return result;
        }
        
        String hostname = normalizeHostname(testUrl.getHost());
        
        int port = testUrl.getPort();
        if (port == testUrl.getDefaultPort()) {
            port = -1;
        }
        
        String path = normalizePath(testUrl.getPath());
        String query = normalizeQuery(testUrl.getQuery());

        try {
            testUrl = new URL(protocol, hostname, port, path + query);
        } catch (MalformedURLException e) {
            // Should never happen, so just log it and return current result
            LOGGER.error("Unexpected exception during normalization", e);
            return result;
        }
        
        return testUrl.toExternalForm();
    }
}
