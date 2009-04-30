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
package bixo.fetcher.http;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.io.BytesWritable;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;

public class HttpClientFetcher implements IHttpFetcher {
    private static Logger LOGGER = Logger.getLogger(HttpClientFetcher.class);

    private static final int ERROR_CONTENT_LENGTH = 1024;
    public static final int BUFFER_SIZE = 8 * 1024;

    private HttpClient _httpClient;
    private HttpContext _httpContext;
    private FetcherPolicy _fetcherPolicy;

    public HttpClientFetcher(HttpClient httpClient, FetcherPolicy fetcherPolicy) {
        _httpClient = httpClient;
        _httpContext = new BasicHttpContext();
        _fetcherPolicy = fetcherPolicy;
    }

    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) {
        HttpGet httpget = null;
        String url = scoredUrl.getUrl();
        
        try {
            LOGGER.trace("Fetching " + url);
            httpget = new HttpGet(new URI(url));
            // FUTURE KKr - support If-Modified-Since header
            // TODO KKr get host from meta-data
//            if (host != null) {
//                // Set the host explicitly, in case we're using IP addresses, so
//                // that
//                // domain handling works on the target server.
//                httpget.addHeader("Host", host);
//            }

            long readStartTime = System.currentTimeMillis();
            HttpResponse response = _httpClient.execute(httpget, _httpContext);
            int statusCode = response.getStatusLine().getStatusCode();

            // Figure out how much data we want to try to fetch.
            int targetLength;
            FetchStatusCode fsCode;
            if (statusCode == HttpStatus.SC_OK) {
                fsCode = FetchStatusCode.FETCHED;
                targetLength = Integer.MAX_VALUE;
                // TODO KKr - limit to max length, based on conf
            } else {
                fsCode = FetchStatusCode.ERROR;
                // Even for an error case, we can use the response body data for
                // debugging.
                targetLength = ERROR_CONTENT_LENGTH;
            }

            Header[] headers = response.getHeaders(HttpHeaders.CONTENT_LENGTH);
            for (Header header : headers) {
                try {
                    int length = Integer.parseInt(header.getValue());
                    targetLength = Math.min(targetLength, length);
                } catch (NumberFormatException e) {
                    // Ignore (and log) invalid content length values.
                    LOGGER.warn("Invalid content length in header: " + header.getValue());
                }
            }

            // Now finally read in response body, up to targetLength bytes.
            // FUTURE KKr - use content-type to exclude/include data, as that's
            // a more accurate
            // way to skip unwanted content versus relying on suffix.
            byte[] content = null;
            long readRate = 0;
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                InputStream in = entity.getContent();

                try {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead = 0;
                    int totalRead = 0;
                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                    int readRequests = 0;
                    int minResponseRate = _fetcherPolicy.getMinResponseRate();
                    // TODO KKr - we need to monitor the rate while reading a
                    // single block. Look at HttpClient
                    // metrics support for how to do this. Once we fix this, fix
                    // the test to read a smaller (< 20K)
                    // chuck of data.
                    while (((bytesRead = in.read(buffer, 0, Math.min(buffer.length, targetLength - totalRead))) != -1) && (totalRead < targetLength)) {
                        readRequests += 1;
                        totalRead += bytesRead;

                        // Assume read time is at least one microsecond, to
                        // avoid DBZ exception.
                        long totalReadTime = Math.max(1, System.currentTimeMillis() - readStartTime);
                        readRate = (totalRead * 1000L) / totalReadTime;

                        // Don't bail on the first read cycle, as we can get a
                        // hiccup starting out.
                        // Also don't bail if we've read everything we need.
                        if ((readRequests > 1) && (totalRead < targetLength) && (readRate < minResponseRate)) {
                            fsCode = FetchStatusCode.ABORTED;
                            safeAbort(httpget);
                            break;
                        }

                        out.write(buffer, 0, bytesRead);
                    }

                    content = out.toByteArray();
                } catch (Throwable t) {
                    // TODO KKr - will get get an interrupted exception here if
                    // we are terminating
                    // the fetch cycle due to hitting a time limit.
                    if (statusCode == HttpStatus.SC_OK) {
                        throw t;
                    }

                    // If we're just trying to read in content for an error
                    // case,
                    // we are OK with empty content
                }
            }

            // Note that getContentType can return null
            String contentType = entity.getContentType().getValue();

            // TODO KKr - handle redirects, real content type, what about
            // charset? Do we need to capture HTTP headers?
            String redirectedUrl = url;
            // TODO SG used the new enum here.Use different status than fetch if
            // you need to.
            return new FetchedDatum(fsCode, url, redirectedUrl, System.currentTimeMillis(), new BytesWritable(content), contentType, (int)readRate, scoredUrl.getMetaDataMap());
        } catch (Throwable t) {
            safeAbort(httpget);

            LOGGER.debug("Exception while fetching url " + url, t);
            // TODO KKr - use real status for exception, include exception msg
            // somehow.
            // TODO SG should we use FetchStatusCode.ERROR or NEVER_FTEHCED?
            return new FetchedDatum(FetchStatusCode.ERROR, url, url, System.currentTimeMillis(), null, null, 0, scoredUrl.getMetaDataMap());
        }
    }

    private static void safeAbort(HttpRequestBase request) {
        try {
            request.abort();
        } catch (Throwable t) {
            // Ignore any errors
        }
    }

}
