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
package bixo.urls;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import bixo.utils.StringUtils;

@SuppressWarnings("serial")
public class SimpleUrlNormalizer extends BaseUrlNormalizer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleUrlNormalizer.class);
    
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
    
    // Match "/xx/../" in the url, where xx consists of chars, different then "/"
    // (slash) and needs to have at least one char different from "."
    // Also match a leading "/../" in the URL. Both can be replaced by just "/"
    private static final Pattern RELATIVE_PATH_PATTERN = Pattern.compile("(/[^/]*[^/.]{1}[^/]*/\\.\\./|^(/\\.\\./)+)");
    
    // Match against default pages such as /index.html, etc. 
    private static final Pattern DEFAULT_PAGE_PATTERN = Pattern.compile("/((?i)index|default)\\.((?i)js[pf]{1}?[afx]?|cgi|cfm|asp[x]?|[psx]?htm[l]?|php[3456]?)(\\?|&|#|$)");
    
    // Remove things that look like the (invalid) jsession ids prefixing or suffixing the query portion of a URL.
    private static final Pattern JSESSION_ID_PATTERN = Pattern.compile("(?:;jsessionid=.*?)(\\?|&|#|$)");
    
    // Remove things that look like session ids from the query portion of a URL.
    private static final Pattern SESSION_ID_PATTERN = Pattern.compile("(\\?|&)(?:(?i)sid|phpsessid|sessionid|session_id|bv_sessionid|jsessionid|-session|session|session_key)=.*?(&|#|$)");
    
    // Remove other common unwanted parameters from the query portion of a URL.
    private static final Pattern OTHER_IGNORED_QUERY_PARAMETERS_PATTERN = Pattern.compile("(\\?|&)(?:(?i)width|format|country|height|src|user|username|uname|return_url|returnurl|sort|sort_by|sortby|sort_direction|sort_key|order_by|orderby|sortorder|collate)=.*?(&|#|$)");
    
    // Remove even more common unwanted parameters from the query portion of a URL.
    private static final Pattern AGGRESSIVE_IGNORED_QUERY_PARAMETERS_PATTERN = Pattern.compile("(\\?|&)(?:(?i)user|usr|user_id|userid|memberid)=.*?(&|#|$)");
    
    private boolean _treatRefAsQuery;
    private boolean _isAggressive;
    
    public SimpleUrlNormalizer() {
    	this(false, false);
    }
    
    public SimpleUrlNormalizer(boolean treatRefAsQuery) {
        this(treatRefAsQuery, false);
    }
    
    public SimpleUrlNormalizer(boolean treatRefAsQuery, boolean isAggressive) {
        _treatRefAsQuery = treatRefAsQuery;
        _isAggressive = isAggressive;
    }
    
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
            LOGGER.error("Unexpected exception during URL encoding: " + e);
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
            LOGGER.error("Unexpected exception during URL decoding: " + e);
            return url;
        }
    }

    
    public String normalizeHostname(String hostname) {
        String result = hostname.toLowerCase();
        // Convert domain.com => www.domain.com
        // TODO - This isn't always valid, so make it an optional step?
//        String paidLevelDomain = DomainNames.getPLD(result);
//        if (result.equals(paidLevelDomain) && !isIPAddress(paidLevelDomain)) {
//            result = "www." + paidLevelDomain;
//        }
        
        if (result.endsWith(".")) {
        	result = result.substring(0, result.length() - 1);
        }
        
        return result;
    }
    
    
    // Now we get to re-encode the path and query portions of the URL, but we have
    // to split up the path as otherwise '/' => %2F.
    public String normalizePath(String path) {
        // First, handle relative paths
        Matcher matcher = RELATIVE_PATH_PATTERN.matcher(path);
        while (matcher.find()) {
            path = path.substring(0, matcher.start()) + "/" + path.substring(matcher.end());
            matcher = RELATIVE_PATH_PATTERN.matcher(path);
        }
        
        // Next, get rid of any default page.
        matcher = DEFAULT_PAGE_PATTERN.matcher(path);
        if (matcher.find()) {
            path = path.substring(0, matcher.start()) + "/" + matcher.group(3) + path.substring(matcher.end());
        }
        
        String[] pathParts = path.split("/");
        
        StringBuilder newPath = new StringBuilder();
        for (String pathPart : pathParts) {
            if (pathPart.length() > 0) {
                newPath.append('/');
                newPath.append(encodeUrlComponent(decodeUrl(pathPart), RESERVED_PATH_CHARS));
            }
        }
        
        if (newPath.length() == 0) {
            return "/";
        }

        // Preserve state of final / in path
        if (path.endsWith("/") && (newPath.charAt(newPath.length() - 1) != '/')) {
            newPath.append('/');
        }
        
        return newPath.toString();
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
            if (queryPart.length() == 0) {
                // Strip out empty query parts, e.g. q=1&&z=2
                continue;
            }
            
            String[] keyValues = StringUtils.splitOnChar(queryPart, '=');
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
        if ((newQuery.length() > 0) && (newQuery.charAt(newQuery.length() - 1) == '&')) {
            newQuery.setLength(newQuery.length() - 1);
        }
        
        return newQuery.toString();
    }

    public String normalize(String url) {
        String result = url.trim();
        
        // First see if there is any protocol - if not, append http:// by default.
        if (result.indexOf("://") == -1) {
            // FUTURE - could put some limit on max length of protocol string.
            result = "http://" + result;
        }
        
        // Danger, hack! Some sites have session ids that look like http://domain.com/page.html;jsessionid=xxx,
        // or even http://domain.com/page.html;jsessionid=xxx&q=z. So we always want to try to get rid of
        // session ids first, before doing any other processing.
        Matcher matcher = JSESSION_ID_PATTERN.matcher(result);
        if (matcher.find()) {
            result = result.substring(0, matcher.start()) + matcher.group(1) + result.substring(matcher.end());
        }
        
        matcher = SESSION_ID_PATTERN.matcher(result);
        if (matcher.find()) {
            result = result.substring(0, matcher.start()) + matcher.group(1) + matcher.group(2) + result.substring(matcher.end());
        }
        
        matcher = OTHER_IGNORED_QUERY_PARAMETERS_PATTERN.matcher(result);
        if (matcher.find()) {
            result = result.substring(0, matcher.start()) + matcher.group(1) + matcher.group(2) + result.substring(matcher.end());
        }
        
        if (_isAggressive) {
            matcher = AGGRESSIVE_IGNORED_QUERY_PARAMETERS_PATTERN.matcher(result);
            if (matcher.find()) {
                result = result.substring(0, matcher.start()) + matcher.group(1) + matcher.group(2) + result.substring(matcher.end());
            }
        }
        
        URL testUrl;
        
        try {
            String decodedUrl = result.replace("+", "%20");
            testUrl = new URL(decodedUrl);
            url = testUrl.toExternalForm();
        } catch (MalformedURLException e) {
            // Not a valid URL we know about, so in this case we're just going to
            // return it as-is, other than the stripping we did.
            LOGGER.debug("Malformed URL being returned without further processing: " + result);
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
        // then we want to include it versus stripping it out. But only do this if the caller
        // explicitly wants that behavior, as most sites use .../#<whatever> for dynamic navigation.
        
        // FUTURE KKr - better would be to not require special param, and instead always see if the
        // ref looks like a query, in that there's one or more <key>=<value> pairs separated by '&'.
        String query = testUrl.getQuery();
        String anchor = testUrl.getRef();
        
        int pos = url.indexOf("#" + anchor);
        if (_treatRefAsQuery && (anchor != null) && (query == null) && (pos != -1) && (url.charAt(pos - 1) == '/')) {
            anchor = "#" + normalizeQuery(anchor);
            query = "";
        } else {
            anchor = "";
            query = normalizeQuery(query);
            
            if (query.length() > 0) {
                query = "?" + query;
            }
        }
        
        try {
            testUrl = new URL(protocol, hostname, port, path + query + anchor);
        } catch (MalformedURLException e) {
            LOGGER.error("Unexpected exception during normalization: " + e);
            return result;
        }
        
        return testUrl.toExternalForm();
    }

}
