/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Extract the PLD (paid-level domain, as per the IRLbot paper) from a hostname. This is
 * used to group URLs when IP-based grouping isn't used.
 *
 */

public class DomainNames {
    private static final String CC_TLDS =
        "ac ad ae af ag ai al am an ao aq ar as at au aw ax az ba bb bd be bf bg bh bi " +
        "bj bl bm bn bo br bs bt bv bw by bz ca cc cd cf cg ch ci ck cl cm cn co cr cu " +
        "cv cx cy cz de dj dk dm do dz ec ee eg eh er es et eu fi fj fk fm fo fr ga gb " +
        "gd ge gf gg gh gi gl gm gn gp gq gr gs gt gu gw gy hk hm hn hr ht hu id ie il " +
        "im in io iq ir is it je jm jo jp ke kg kh ki km kn kp kr kw ky kz la lb lc li " +
        "lk lr ls lt lu lv ly ma mc md me mf mg mh mk ml mm mn mo mp mq mr ms mt mu mv " +
        "mw mx my mz na nc ne nf ng ni nl no np nr nu nz om pa pe pf pg ph pk pl pm pn " +
        "pr ps pt pw py qa re ro rs ru rw sa sb sc sd se sg sh si sj sk sl sm sn so sr " +
        "st su sv sy sz tc td tf tg th tj tk tl tm tn to tp tr tt tv tw tz ua ug uk um " +
        "us uy uz va vc ve vg vi vn vu wf ws ye yt yu za zm zw";

    private static final String G_TLDS = 
        "aero arpa asia biz cat com coop edu gov info int jobs mil mobi museum name net " +
        "org pro tel";

    private static final Set<String> ccTLDs = new HashSet<String>(Arrays.asList(CC_TLDS.split(" ")));
    private static final Set<String> gTLDs = new HashSet<String>(Arrays.asList(G_TLDS.split(" ")));

    private static final Pattern IPV4_ADDRESS_PATTERN = Pattern.compile("(?:\\d{1,3}\\.){3}\\d{1,3}");

    // TODO CSc Rename this getDomainPld?
    /**
     * Extract the PLD (paid-level domain) from the hostname. If the format isn't recognized,
     * the original hostname is returned.
     * 
     * @param hostname - hostname from URL, e.g. www.domain.com.it
     * @return - PLD, e.g. domain.com.it
     */
    public static String getPLD(String hostname) {
        // First, check for weird [HHHH:HH::H] IPv6 format.
        if (hostname.startsWith("[") && hostname.endsWith("]")) {
            return hostname;
        }

        String[] subNames = hostname.split("\\.");
        int numPieces = subNames.length;
        if (numPieces <= 2) {
            return hostname;
        }

        // Check for ddd.ddd.ddd.ddd IPv4 format
        if ((numPieces == 4) && (IPV4_ADDRESS_PATTERN.matcher(hostname).matches())) {
            return hostname;
        }

        int firstHostPiece = 0;
        if (ccTLDs.contains(subNames[numPieces - 1].toLowerCase())) {
            // We have a country code at the end. See if the preceding piece is either
            // a two-letter name (country code or funky short gTLD), or one of the
            // "well-known" gTLDs.
            if (subNames[numPieces - 2].length() <= 2) {
                // Must be xxx.co.jp format
                firstHostPiece = numPieces - 3;
            } else if (gTLDs.contains(subNames[numPieces - 2].toLowerCase())) {
                // Must be xxx.com.mx format
                firstHostPiece = numPieces - 3;
            } else {
                // Must be xxx.it format
                firstHostPiece = numPieces - 2;
            }
        } else if (gTLDs.contains(subNames[numPieces - 1].toLowerCase())) {
            if (ccTLDs.contains(subNames[numPieces - 2].toLowerCase())) {
                // Must be xxx.de.com format
                firstHostPiece = numPieces - 3;
            } else {
                // Must be xxx.com format
                firstHostPiece = numPieces - 2;
            }
        } else {
            // Unknown format.
            // TODO KKr - log this?
        }

        if (firstHostPiece == 0) {
            return hostname;
        } else {
            // Build the result from the firstHostPiece to numPices pieces.
            StringBuilder result = new StringBuilder();
            for (int i = firstHostPiece; i < numPieces; i++) {
                result.append(subNames[i]);
                result.append('.');
            }

            // Trim off final '.'
            return result.deleteCharAt(result.length() - 1).toString();
        }
    } // getPLD


    // TODO CSc Rename this getUrlPld (or getURLPld)? Add a getUrlStringPld?
    /**
     * Extract the PLD (paid-level domain) from the URL.
     * 
     * @param url - Valid URL, e.g. http://www.domain.com.it
     * @return - PLD e.g. domain.com.it
     */
    public static String getPLD(URL url) {
        return getPLD(url.getHost());
    } // getPLD
    
    
    /**
     * No-exception utility routine to return the hostname for a URL.
     * 
     * @param url
     * @return hostname, or <invalid URL>.
     */
    public static String safeGetHost(String url) {
        try {
            return new URL(url).getHost();
        } catch (MalformedURLException e) {
            return "<invalid URL>";
        }
    }
    
    /**
     * Extract the domain immediately containing this subdomain.
     * 
     * @param hostname 
     * @return immediate super domain of hostname, or null if hostname
     * is already a paid-level domain (i.e., not really a subdomain).
     */
    public static String getSuperDomain(String hostname) {
        String pld = getPLD(hostname);
        if (hostname.equalsIgnoreCase(pld)) {
            return null;
        }
        return hostname.substring(hostname.indexOf(".")+1);
    }
    
    /**
     * Check whether the domain of the URL is the given domain or a subdomain
     * of the given domain.
     * 
     * @param url
     * @param domain
     * @return true iff url is "within" domain
     */
    public static boolean isUrlWithinDomain(String url, String domain) {
        try {
            for (   String urlDomain = new URL(url).getHost();
                    urlDomain != null;
                    urlDomain = DomainNames.getSuperDomain(urlDomain)) {
                if (urlDomain.equalsIgnoreCase(domain)) {
                    return true;
                }
            }
        } catch (MalformedURLException e) {
            return false;
        }
        return false;
    }
    
    /**
     * Check whether this paid level domain is just a naked IP address.
     * 
     * @param paidLevelDomain
     * @return true iff paidLevelDomain is a naked IP address
     */
    public static boolean isIPAddress(String paidLevelDomain) {
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

} // DomainNames
