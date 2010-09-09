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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.ContentBytes;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.Payload;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.FetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.UrlFetchException;
import bixo.fetcher.http.IHttpFetcher;
import bixo.utils.ConfigUtils;

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

    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) throws FetchException {
        return doGet(scoredUrl.getUrl(), scoredUrl.getPayload(), true);
    }

    @Override
    public byte[] get(String url) throws FetchException {
        try {
            FetchedDatum result = doGet(url, new Payload(), true);
            return result.getContentBytes();
        } catch (HttpFetchException e) {
            if (e.getHttpStatus() == HttpStatus.SC_NOT_FOUND) {
                return new byte[0];
            } else {
                throw e;
            }
        }
    }
    
    private FetchedDatum doGet(String url, Payload payload, boolean returnContent) throws FetchException {
        LOGGER.trace("Fake fetching " + url);
        
        URL theUrl;
        try {
            theUrl = new URL(url);
        } catch (MalformedURLException e) {
            throw new UrlFetchException(url, e.getMessage());
        }
        
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
            throw new HttpFetchException(url, "Exception requested from FakeHttpFetcher", statusCode, null);
        }

        if (!returnContent) {
            contentSize = 0;
        }
        
        // Now we want to delay for as long as it would take to fill in the data.
        float duration = (float) contentSize / (float) bytesPerSecond;
        LOGGER.trace(String.format("Fake fetching %d bytes at %d bps (%fs) from %s", contentSize, bytesPerSecond, duration, url));
        try {
            Thread.sleep((long) (duration * 1000.0));
        } catch (InterruptedException e) {
            // Break out of our delay, but preserve interrupt state.
            Thread.currentThread().interrupt();
        }

        HttpHeaders headers = new HttpHeaders();
        headers.add("x-responserate", "" + bytesPerSecond);
        FetchedDatum result = new FetchedDatum(url, url, System.currentTimeMillis(), headers, new ContentBytes(new byte[contentSize]), "text/html", bytesPerSecond);
        result.setPayload(payload);
        return result;
    }

	@Override
	public UserAgent getUserAgent() {
		return ConfigUtils.BIXO_TEST_AGENT;
	}
	
    @Override
    public void abort() {
        // Do nothing
    }

}
