package bixo.utils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

public class URLNormalizer {
    private static final Logger LOGGER = Logger.getLogger(URLNormalizer.class);
    
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
            decodedUrl = URLDecoder.decode(decodedUrl, "UTF-8");
            testUrl = new URL(decodedUrl);
        } catch (MalformedURLException e) {
            // Not a valid URL we know about, so in this case we're just going to
            // return it as-is, other than the stripping we did.
            return result;
        } catch (UnsupportedEncodingException e) {
            // Should never happen, so log it and return what we've got.
            LOGGER.error("Unexpected exception during URL decoding", e);
            return result;
        }
        
        // Don't do additional special processing for anything other than http/https protocols.
        String protocol = testUrl.getProtocol().toLowerCase();
        if (!protocol.equals("http") && !protocol.equals("https")) {
            return result;
        }
        
        // Convert domain.com => www.domain.com
        String hostname = testUrl.getHost().toLowerCase();
        String paidLevelDomain = DomainNames.getPLD(hostname);
        if (hostname.equals(paidLevelDomain)) {
            hostname = "www." + paidLevelDomain;
        }
        
        int port = testUrl.getPort();
        if (port == testUrl.getDefaultPort()) {
            port = -1;
        }
        
        String file = testUrl.getFile();
        String[] parts = file.split("/");
        
        for (String part : parts) {
        
        }
        
        try {
            file = URLEncoder.encode(file, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Should never happen, so log it
            LOGGER.error("Unexpected exception during URL encoding", e);
        }
        
        if (file.equals("")) {
            file = "/";
        }

        try {
            testUrl = new URL(protocol, hostname, port, file);
        } catch (MalformedURLException e) {
            // Should never happen, so just log it and return current result
            LOGGER.error("Unexpected exception during normalization", e);
            return result;
        }
        
        return testUrl.toExternalForm();
    }
}
