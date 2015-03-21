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
package bixo.parser;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.tika.utils.CharsetUtils;

import bixo.config.ParserPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.fetcher.HttpHeaderNames;
import bixo.utils.HttpUtils;

@SuppressWarnings("serial")
public abstract class BaseParser implements Serializable {

    private ParserPolicy _policy;
    
    public BaseParser(ParserPolicy policy) {
        _policy = policy;
    }

    public ParserPolicy getParserPolicy() {
        return _policy;
    }

    public abstract ParsedDatum parse(FetchedDatum fetchedDatum) throws Exception;

    /**
     * Extract encoding from content-type
     * 
     * If a charset is returned, then it's a valid/normalized charset name that's
     * supported on this platform.
     * 
     * @param datum
     * @return charset in response headers, or null
     */
    protected String getCharset(FetchedDatum datum) {
        return CharsetUtils.clean(HttpUtils.getCharsetFromContentType(datum.getContentType()));
    }

    /**
     * Extract language from (first) explicit header
     * 
     * @param fetchedDatum
     * @param charset 
     * @return first language in response headers, or null
     */
    protected String getLanguage(FetchedDatum fetchedDatum, String charset) {
        return fetchedDatum.getHeaders().getFirst(HttpHeaderNames.CONTENT_LANGUAGE);
    }

    /**
     * Figure out the right base URL to use, for when we need to resolve relative URLs.
     * 
     * @param fetchedDatum
     * @return the base URL
     * @throws MalformedURLException
     */
    protected URL getContentLocation(FetchedDatum fetchedDatum) throws MalformedURLException {
        URL baseUrl = new URL(fetchedDatum.getFetchedUrl());
        
        // See if we have a content location from the HTTP headers that we should use as
        // the base for resolving relative URLs in the document.
        String clUrl = fetchedDatum.getHeaders().getFirst(HttpHeaderNames.CONTENT_LOCATION);
        if (clUrl != null) {
            // FUTURE KKr - should we try to keep processing if this step fails, but
            // refuse to resolve relative links?
            baseUrl = new URL(baseUrl, clUrl);
        }
        return baseUrl;
    }


}
