/*
 * Copyright (c) 2010 TransPac Software, Inc.
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
package bixo.examples.crawl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlFilter;

// Filter URLs that fall outside of the target domain
@SuppressWarnings("serial")
public class DomainUrlFilter extends BaseUrlFilter {
    private static final Logger LOGGER = Logger.getLogger(DomainUrlFilter.class);

    private String _domain;
    private Pattern _suffixExclusionPattern;
    private Pattern _protocolInclusionPattern;

    public DomainUrlFilter(String domain) {
        _domain = domain;
        _suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe)$");
        _protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");
    }

    @Override
    public boolean isRemove(UrlDatum datum) {
        String urlAsString = datum.getUrl();
        
        // Skip URLs with protocols we don't want to try to process
        if (!_protocolInclusionPattern.matcher(urlAsString).find()) {
            return true;
            
        }

        if (_suffixExclusionPattern.matcher(urlAsString).find()) {
            return true;
        }
        
        try {
            URL url = new URL(urlAsString);
            String host = url.getHost();
            return (!host.endsWith(_domain));
        } catch (MalformedURLException e) {
            LOGGER.warn("Invalid URL: " + urlAsString);
            return true;
        }
    }
}
