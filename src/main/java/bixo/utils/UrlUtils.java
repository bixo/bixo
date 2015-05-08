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
package bixo.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UrlUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(UrlUtils.class);

    private static final Pattern IGNORED_PROTOCOL_PATTERN = Pattern.compile("(?i)^(javascript|mailto|about):");
    
    private UrlUtils() {
        // Enforce class isn't instantiated
    }
    
    // TODO VMa : MalformedURLException is being caught but not re-thrown here (SimpleParser is depending on it being thrown)
    public static String makeUrl(URL baseUrl, String relativeUrl) throws MalformedURLException {
        // Peel off cases of URLs that aren't actually URLs, or at least don't have protocols
        // that the Java URL class knows about.
        if (IGNORED_PROTOCOL_PATTERN.matcher(relativeUrl).find()) {
            return relativeUrl;
        }

        // We need to handle one special case, where the relativeUrl is just
        // a query string (like "?pid=1"), and the baseUrl doesn't end with
        // a '/'. In that case, the URL class removes the last portion of
        // the path, which we don't want.

        try {
            if (!relativeUrl.startsWith("?") || (baseUrl.getPath().length() == 0)
                            || baseUrl.getPath().endsWith("/")) {
                return new URL(baseUrl, relativeUrl).toExternalForm();
            } else {
                // for the <file> param, use the base path (which won't include
                // the query string) plus
                // the relative query string.
                return new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(), baseUrl
                                .getPath()
                                + relativeUrl).toExternalForm();
            }
        } catch (MalformedURLException e) {
            // we can get things like "mail:xxx" (versus mailto:) in href
            // attributes.
            LOGGER.warn("Invalid relativeUrl parameter: " + relativeUrl);
            return relativeUrl;
        }
    }
    
    public static String makeProtocolAndDomain(String urlAsString) throws MalformedURLException {
        URL url = new URL(urlAsString);
        StringBuilder result = new StringBuilder(url.getProtocol());
        result.append("://");
        
        String host = url.getHost();
        if (host.length() == 0) {
            throw new MalformedURLException("URL without a domain: " + urlAsString);
        }
        
        result.append(host);
        int port = url.getPort();
        if ((port != -1) && (port != url.getDefaultPort())) {
            result.append(':');
            result.append(port);
        }
        
        return result.toString();
    }
}
