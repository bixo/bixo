/*
 * Copyright 2013-2015 Scale Unlimited
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
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.utils.DomainNames;
import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.HttpFetchException;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.UrlFetchException;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class FakeRobotsFetcher extends BaseHttpFetcher {
    private static Logger LOGGER = LoggerFactory.getLogger(FakeRobotsFetcher.class);
    private static final UserAgent ROBOTS_TEST_AGENT = new UserAgent("test", "test@domain.com", "http://test.domain.com");

    
    private boolean _randomFetching;
    private Random _rand;

    public FakeRobotsFetcher(int maxThreads) {
        this(maxThreads, ROBOTS_TEST_AGENT, true);
    }

    public FakeRobotsFetcher(int maxThreads, UserAgent userAgent) {
        this(maxThreads, userAgent, true);
    }

    public FakeRobotsFetcher(int maxThreads, UserAgent userAgent, boolean randomFetching) {
        super(maxThreads, userAgent);
        _randomFetching = randomFetching;
        _rand = new Random();
    }

    @Override
    public void abort() {
        // Do nothing
    }

    @Override
    public FetchedResult get(String url, Payload payload) throws BaseFetchException {
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

        // Now we want to delay for as long as it would take to fill in the data.
        float duration = (float) contentSize / (float) bytesPerSecond;
        LOGGER.trace(String.format("Fake fetching %d bytes at %d bps (%fs) from %s", contentSize, bytesPerSecond, duration, url));
        try {
            Thread.sleep((long) (duration * 1000.0));
        } catch (InterruptedException e) {
            // Break out of our delay, but preserve interrupt state.
            Thread.currentThread().interrupt();
        }

        Metadata headers = new Metadata(); 
        headers.add("x-responserate", "" + bytesPerSecond);
        FetchedResult result = new FetchedResult(url, url, System.currentTimeMillis(),
                        headers, new byte[contentSize], "text/html", bytesPerSecond,
                        payload,
                        url,
                        0,
                        DomainNames.safeGetHost(url), statusCode, "");
        return result;
    }
    
}
