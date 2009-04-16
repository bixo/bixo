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

import org.apache.log4j.Logger;

import cascading.tuple.TupleEntryCollector;

import bixo.fetcher.beans.FetchItem;
import bixo.tuple.FetchResultTuple;

public class FetcherRunnable implements Runnable {
    private static Logger LOGGER = Logger.getLogger(FetcherRunnable.class);

    private IHttpFetcher _httpFetcher;
    private FetchList _items;

    public FetcherRunnable(IHttpFetcher httpFetcher, FetchList items) {
        _httpFetcher = httpFetcher;
        _items = items;
    }

    @Override
    public void run() {

        // FUTURE KKr - when fetching the last item, send a Connection: close
        // header to let the server know it doesn't need to keep the socket open.
        for (FetchItem item : _items) {
            try {
                FetchResultTuple result = _httpFetcher.get(item.getUrl());
                LOGGER.trace("Fetched " + result);
                TupleEntryCollector collector = item.getCollector();
                
                // Cascading _collectors aren't thread-safe.
                synchronized (collector) {
                    collector.add(result.toTuple());
                }
            } catch (Throwable t) {
                LOGGER.error("Exception: " + t.getMessage(), t);
            }
        }

        // All done fetching these items, so we're no longer hitting this
        // domain.
        _items.finished();
    }

}
