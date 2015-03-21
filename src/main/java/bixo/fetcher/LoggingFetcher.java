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
package bixo.fetcher;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.Payload;


import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.ContentBytes;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.UrlFetchException;

@SuppressWarnings("serial")
public class LoggingFetcher extends BaseFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFetcher.class);
    
    public static final String FAKE_CONTENT_LOCATION = "Fake-LoggingFetcher";
    
    // Generic HTML page we send back for every request - only customization is the URL
    private static final String HTML_TEMPLATE =
        "<!DOCTYPE HTML PUBLIC \"-//BBSW//DTD Compact HTML 2.0//EN\">\n" +
        "<html><head><meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\">\n" +
        "<title>LoggingFetcher</title>\n" +
        "</head><body>URL = %s</body></html>\n";
    
    public LoggingFetcher(int maxThreads) {
        super(maxThreads, new FetcherPolicy(), new UserAgent("agentName", "agentName@domain.com", "http://agentName.domain.com"));
    }


    @Override
    public FetchedDatum get(ScoredUrlDatum datum) throws BaseFetchException {
        String url = datum.getUrl();
        Payload payload = datum.getPayload();
        logPayload(url, payload);
        
        // Create a simple HTML page here, where we fill in the URL as
        // the field, and return that as the BytesWritable. we could add
        // more of the datum values to the template if we cared.
        try {
            return makeFetchedDatum(url, String.format(HTML_TEMPLATE, url), payload);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Should never happen", e);
        } catch (MalformedURLException e) {
            throw new UrlFetchException(url, e.getMessage());
        }
    }

    private FetchedDatum makeFetchedDatum(String url, String htmlContent, Payload payload) throws MalformedURLException, HttpFetchException, UnsupportedEncodingException {
        URL theUrl = new URL(url);
        if (theUrl.getFile().equals("/robots.txt")) {
            throw new HttpFetchException(url, "Never return robots.txt from LoggingFetcher", HttpStatus.SC_NOT_FOUND, null);
        }
        
        byte[] content = htmlContent.getBytes("UTF-8");
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_LENGTH, "" + content.length);
        headers.add(HttpHeaderNames.CONTENT_TYPE, "text/html");
        
        // Set the location to a fixed value, so that when we're processing entries from
        // the URL DB that might have been set using fake content, we know to ignore the
        // refetch time if we're doing a real fetch.
        headers.add(HttpHeaderNames.CONTENT_LOCATION, FAKE_CONTENT_LOCATION);
        FetchedDatum result = new FetchedDatum(url, url, System.currentTimeMillis(), headers, new ContentBytes(content), "text/html", 100000);
        result.setPayload(payload);
        return result;
    }


    private void logPayload(String url, Payload payload) {
        StringBuilder msg = new StringBuilder(url);
        msg.append(" ( ");
        for (String key : payload.keySet()) {
            msg.append(key);
            msg.append(':');
            Object value = payload.get(key);
            msg.append(value == null ? "null" : value.toString());
            msg.append(' ');
        }
        msg.append(")");

        LOGGER.info(msg.toString());
    }

    @Override
    public void abort() {
        // Do nothing
    }

}
