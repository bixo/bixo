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
package bixo.fetcher.simulation;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BixoFetchException;
import bixo.fetcher.http.IHttpFetcher;

@SuppressWarnings("serial")
public class FakeHttpFetcher implements IHttpFetcher {
    private static Logger LOGGER = Logger.getLogger(FakeHttpFetcher.class);

    private boolean _randomFetching;
    private int _maxThreads;
    private FetcherPolicy _fetcherPolicy;
    private Random _rand;

    public FakeHttpFetcher() {
        this(true, 1, new FetcherPolicy());
    }

    public FakeHttpFetcher(boolean randomFetching, int maxThreads) {
        this(randomFetching, maxThreads, new FetcherPolicy());
    }
    
    public FakeHttpFetcher(boolean randomFetching, int maxThreads, FetcherPolicy fetcherPolicy) {
        _randomFetching = randomFetching;
        _maxThreads = maxThreads;
        _fetcherPolicy = fetcherPolicy;
        _rand = new Random();
    }
    
    @Override
    public int getMaxThreads() {
        return _maxThreads;
    }

    @Override
    public FetcherPolicy getFetcherPolicy() {
        return _fetcherPolicy;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) {
        String url = scoredUrl.getUrl();
        Map<String, Comparable> metaData = scoredUrl.getMetaDataMap();
        
        try {
            return doGet(new URL(url), metaData);
        } catch (MalformedURLException e) {
            return FetchedDatum.createErrorDatum(url, e.getMessage(), metaData);
        } catch (Exception e) {
            LOGGER.debug("Exception while fetching url: " + url, e);
            
            // TODO KKr - use real status for exception, include exception msg somehow. Could create a more
            // generic fetch status, that has a fetch status code, http status, message, etc. and a call to
            // return true if the fetch succeeded, and another to return true if the fetch failed. Both might
            // be false if the fetch was aborted, for example. Cheesy hack is to use a special header value
            // that has the exception in it. Though putting errors inside of "fetched datum" seems a bit odd
            // to begin with, as this is something that hasn't actually been fetched.

            return FetchedDatum.createErrorDatum(url, e.getClass().getSimpleName() + ": " + e.getMessage(), metaData);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] get(String url) throws IOException, URISyntaxException, BixoFetchException {
        FetchedDatum result = doGet(new URL(url), new HashMap<String, Comparable>());
        
        if (result.getHttpStatus() == HttpStatus.SC_NOT_FOUND) {
            return new byte[0];
        } else if (result.getHttpStatus() == HttpStatus.SC_OK) {
            return result.getContent().getBytes();
        } else {
            throw new BixoFetchException(result.getHttpStatus(), result.getHttpMsg());
        }
    }
    
    @SuppressWarnings("unchecked")
    private FetchedDatum doGet(URL theUrl, Map<String, Comparable> metaData) throws BixoFetchException {
        int statusCode = HttpStatus.SC_OK;
        int contentSize = 10000;
        int bytesPerSecond = 100000;

        if (_randomFetching) {
            contentSize = Math.max(0, (int) (_rand.nextGaussian() * 5000.0) + 10000) + 100;
            bytesPerSecond = Math.max(0, (int) (_rand.nextGaussian() * 25000.0) + 50000) + 1000;
        } else {
            String query = theUrl.getQuery();
            if (query != null) {
                String[] params = query.split("&");
                for (String param : params) {
                    String[] keyValue = param.split("=");
                    if (keyValue[0].equals("status")) {
                        statusCode = Integer.parseInt(keyValue[1]);
                    } else if (keyValue[0].equals("size")) {
                        contentSize = Integer.parseInt(keyValue[1]);
                    } else if (keyValue[0].equals("speed")) {
                        bytesPerSecond = Integer.parseInt(keyValue[1]);
                    } else {
                        LOGGER.warn("Unknown fake URL parameter: " + keyValue[0]);
                    }
                }
            }
        }

        if (statusCode != HttpStatus.SC_OK) {
            throw new BixoFetchException(statusCode, "Exception requests from FakeHttpFetcher");
        }
        
        // Now we want to delay for as long as it would take to fill in the data.
        String url = theUrl.toExternalForm();
        float duration = (float) contentSize / (float) bytesPerSecond;
        LOGGER.trace(String.format("Fake fetching %d bytes at %d bps (%fs) from %s", contentSize, bytesPerSecond, duration, url));
        try {
            Thread.sleep((long) (duration * 1000.0));
        } catch (InterruptedException e) {
            // Break out of our delay, but preserve interrupt state.
            Thread.currentThread().interrupt();
        }
        
        return new FetchedDatum(FetchStatusCode.FETCHED, HttpStatus.SC_OK, url, url, System.currentTimeMillis(), null, new BytesWritable(new byte[contentSize]), "text/html", bytesPerSecond, metaData);
    }
}
