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
package bixo.fetcher;

import java.net.URL;
import java.util.Random;

import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchContentTuple;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;

public class FakeHttpFetcher implements IHttpFetcher {
    private static Logger LOGGER = Logger.getLogger(FakeHttpFetcher.class);
    
    private boolean _randomFetching;
    private FetcherPolicy _fetcherPolicy;
    private Random _rand;
    
    public FakeHttpFetcher() {
        this(true, new FetcherPolicy());
    }
    
    public FakeHttpFetcher(boolean randomFetching, FetcherPolicy fetcherPolicy) {
        _randomFetching = randomFetching;
        _fetcherPolicy = fetcherPolicy;
        _rand = new Random();
    }
    
    @Override
    public FetchedDatum get(String url) {
        return get(url, null);
    }

    @Override
    public FetchedDatum get(String url, String host) {
        try {
            URL theUrl = new URL(url);
            
            int statusCode = 200;
            int contentSize = 10000;
            int bytesPerSecond = 100000;
            
            if (_randomFetching) {
                contentSize = Math.max(0, (int)(_rand.nextGaussian() * 5000.0) + 10000) + 100;
                bytesPerSecond = Math.max(0, (int)(_rand.nextGaussian() * 25000.0) + 50000) + 1000;
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
            
            FetchStatusCode status = statusCode == 200 ? FetchStatusCode.FETCHED : FetchStatusCode.ERROR;
            FetchContentTuple content = new FetchContentTuple(url, url, System.currentTimeMillis(), new byte[contentSize], "text/html", bytesPerSecond);
            
            // Now we want to delay for as long as it would take to fill in the data.
            float duration = (float)contentSize/(float)bytesPerSecond;
            LOGGER.trace(String.format("Fake fetching %d bytes at %d bps (%fs) from %s", contentSize, bytesPerSecond, duration, url));
            Thread.sleep((long)(duration * 1000.0));
            return new FetchedDatum(status, content);
        } catch (Throwable t) {
            LOGGER.error("Exception: " + t.getMessage(), t);
            return new FetchedDatum(FetchStatusCode.ERROR, new FetchContentTuple(url, url, System.currentTimeMillis(), null, null, 0));
        }
    }


}
