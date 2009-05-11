package bixo.urldb;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import bixo.utils.DomainNames;

@SuppressWarnings("serial")
public class UrlNormalizer implements IUrlNormalizer {
    private static final Logger LOGGER = Logger.getLogger(UrlNormalizer.class);
    
    // http://en.wikipedia.org/wiki/Percent-encoding - full set of reserved chars is:
    // !    *   '   (   )   ;   :   @   &   =   +   $   ,   /   ?   %   #   [   ]
    // But you only need to encode "reserved purpose" characters, and that sub-set of
    // the reserved chars varies depending upon the protocol and the component. Since
    // we only are really worried about normalizing http(s) URLs
    // Not really sure about ':' and '?' being reserved in queries, but that's what StumbleUpon thinks, and that's
    // who we need to support, so...
    private static final String RESERVED_QUERY_CHARS = "%&;=:?#";

    private static final String RESERVED_PATH_CHARS = "%/?#";
    
    private static final String HEX_CODES = "0123456789abcdefABCDEF";
    
    private String encodeCodePoint(int codepoint) {
        try {
            int[] codepoints = { codepoint };
            byte[] bytes = new String(codepoints, 0, 1).getBytes("UTF-8");
            
            StringBuilder result = new StringBuilder();
            for (byte value : bytes) {
                result.append(String.format("%%%02x", value));
            }
            
            return result.toString();
        } catch (UnsupportedEncodingException e) {
            // Should never happen, so log it.
            LOGGER.error("Unexpected exception during URL encoding", e);
            return "";
        }

    }
    
    private String encodeUrlComponent(String component, String reservedChars) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < component.length(); ) {
            int codePoint = component.codePointAt(i);
            if (codePoint == 0x0020) {
                result.append('+');
            } else if (codePoint >= 0x007F) {
                result.append(encodeCodePoint(codePoint));
            } else if ((codePoint < 0x0020) || (reservedChars.indexOf((char)codePoint) != -1)) {
                result.append(String.format("%%%02x", codePoint));
            } else {
                result.append((char)codePoint);
            }
            
            i += Character.charCount(codePoint);
        }
        
        return result.toString();
    }
    
    public String decodeUrl(String url) {
        // FUTURE - handle unsupported %uHHHH sequences for Unicode code points.
        // FUTURE - detect & handle incorrectly encoded URLs
        
        // First, try to catch unescaped '%' characters.
        int offset = 0;
        while ((offset = url.indexOf('%', offset)) != -1) {
            offset += 1;
            boolean needsEscaping = false;
            if (offset > (url.length() - 2)) {
                needsEscaping = true;
            } else if ((HEX_CODES.indexOf(url.charAt(offset)) == -1) || (HEX_CODES.indexOf(url.charAt(offset + 1)) == -1)) {
                needsEscaping = true;
            }
            
            if (needsEscaping) {
                url = url.substring(0, offset) + "25" + url.substring(offset);
                offset += 1;
            }
        }
        
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
        if (result.equals(paidLevelDomain) && !isIPAddress(paidLevelDomain)) {
            result = "www." + paidLevelDomain;
        }
        
        return result;
    }
    
    
    private boolean isIPAddress(String paidLevelDomain) {
        // FUTURE - Handle ipV6 addresses.
        String[] pieces = paidLevelDomain.split("\\.");
        if (pieces.length != 4) {
            return false;
        }
        
        for (String octet : pieces) {
            try {
                int value = Integer.parseInt(octet);
                if ((value < 0) || (value > 255)) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        
        return true;
    }

    // Now we get to re-encode the path and query portions of the URL, but we have
    // to split up the path as otherwise '/' => %2F.
    public String normalizePath(String path) {
        String[] pathParts = path.split("/");
        
        StringBuilder newPath = new StringBuilder();
        for (String pathPart : pathParts) {
            newPath.append(encodeUrlComponent(decodeUrl(pathPart), RESERVED_PATH_CHARS));
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
            String[] keyValues = splitOnChar(queryPart, '=');
            if (keyValues.length == 1) {
                newQuery.append(encodeUrlComponent(decodeUrl(keyValues[0]), RESERVED_QUERY_CHARS));
                if (queryPart.endsWith("=")) {
                    newQuery.append("=");
                }
            } else {
                for (String kvPart : keyValues) {
                    newQuery.append(encodeUrlComponent(decodeUrl(kvPart), RESERVED_QUERY_CHARS));
                    newQuery.append('=');
                }

                newQuery.setLength(newQuery.length() - 1);
            }

            newQuery.append('&');
        }

        // Remove last '&'
        newQuery.setLength(newQuery.length() - 1);
        return newQuery.toString();
    }

    // Do our own version of String.split(), which returns every section even if
    // it's empty. This then satisfies what we need, namely:
    //
    // "a=b" => "a" "b"
    // "" => ""
    // "=" => "" ""
    // "a=" => "a" ""
    // "a==" => "a" "" ""
    private String[] splitOnChar(String str, char c) {
        ArrayList<String> result = new ArrayList<String>();
        
        int lastOffset = 0;
        int curOffset;
        while ((curOffset = str.indexOf(c, lastOffset)) != -1) {
            result.add(str.substring(lastOffset, curOffset));
            lastOffset = curOffset + 1;
        }
        
        result.add(str.substring(lastOffset));
        
        return result.toArray(new String[result.size()]);
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
            String decodedUrl = result.replace("+", "%20");
            testUrl = new URL(decodedUrl);
        } catch (MalformedURLException e) {
            // Not a valid URL we know about, so in this case we're just going to
            // return it as-is, other than the stripping we did.
            LOGGER.trace("Malformed URL being returned without further processing: " + result);
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
        
        // Danger, hack! Some sites (like StumbleUpon) use anchor text as query text, so they
        // have a URL that looks like http://www.stumbleupon.com/toolbar/#url=...
        // Assume that if the first '#' is preceded by a '/', and that '#' is our anchor text,
        // then we want to include it versus stripping it out.
        String query = testUrl.getQuery();
        String anchor = testUrl.getRef();
        
        int pos = url.indexOf("#" + anchor);
        if ((anchor != null) && (query == null) && (pos != -1) && (url.charAt(pos - 1) == '/')) {
            anchor = "#" + normalizeQuery(anchor);
            query = "";
        } else if (query != null) {
            anchor = "";
            query = "?" + normalizeQuery(query);
        } else {
            anchor = "";
            query = "";
        }
        
        try {
            testUrl = new URL(protocol, hostname, port, path + query + anchor);
        } catch (MalformedURLException e) {
            // Should never happen, so just log it and return current result
            LOGGER.error("Unexpected exception during normalization", e);
            return result;
        }
        
        return testUrl.toExternalForm();
    }


}
