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
package bixo.fetcher.simulation;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.Payload;


import bixo.config.FetcherPolicy;
import bixo.datum.ContentBytes;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.UrlFetchException;
import bixo.fetcher.BaseFetcher;
import bixo.utils.ConfigUtils;

@SuppressWarnings("serial")
public class FakeHttpFetcher extends BaseFetcher {
    private static Logger LOGGER = LoggerFactory.getLogger(FakeHttpFetcher.class);

    private boolean _randomFetching;
    private Random _rand;

    public FakeHttpFetcher() {
        this(true, 1, new FetcherPolicy());
    }

    public FakeHttpFetcher(boolean randomFetching, int maxThreads) {
        this(randomFetching, maxThreads, new FetcherPolicy());
    }
    
    public FakeHttpFetcher(boolean randomFetching, int maxThreads, FetcherPolicy fetcherPolicy) {
        super(maxThreads, fetcherPolicy, ConfigUtils.BIXO_TEST_AGENT);
        
        _randomFetching = randomFetching;
        _rand = new Random();
    }
    
    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException {
        return doGet(scoredUrl.getUrl(), scoredUrl.getPayload(), true);
    }

    private FetchedDatum doGet(String url, Payload payload, boolean returnContent) throws BaseFetchException {
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
    public void abort() {
        // Do nothing
    }

}
