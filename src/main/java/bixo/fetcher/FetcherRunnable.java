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

import org.apache.log4j.Level;

import bixo.cascading.BixoFlowProcess;
import bixo.fetcher.beans.FetchStatusCode;
import bixo.tuple.FetchedDatum;
import bixo.tuple.ScoredUrlDatum;
import cascading.tuple.TupleEntryCollector;

public class FetcherRunnable implements Runnable {
    private IHttpFetcher _httpFetcher;
    private FetchList _items;

    public FetcherRunnable(IHttpFetcher httpFetcher, FetchList items) {
        _httpFetcher = httpFetcher;
        _items = items;
    }

    @Override
    public void run() {
        BixoFlowProcess process = _items.getProcess();

        // FUTURE KKr - when fetching the last item, send a Connection: close
        // header to let the server know it doesn't need to keep the socket open.
        for (ScoredUrlDatum item : _items) {
            boolean fetching = false;
            
            try {
                fetching = true;
                process.increment(FetcherCounters.URLS_FETCHING, 1);
                long startTime = System.currentTimeMillis();
                FetchedDatum result = _httpFetcher.get(item);
                long deltaTime = System.currentTimeMillis() - startTime;
                process.decrement(FetcherCounters.URLS_FETCHING, 1);
                process.increment(FetcherCounters.FETCHED_TIME, (int)deltaTime);
                fetching = false;
                
                if (result.getStatusCode() == FetchStatusCode.FETCHED) {
                    process.increment(FetcherCounters.URLS_FETCHED, 1);
                    process.increment(FetcherCounters.FETCHED_BYTES, result.getContent().getLength());
                    process.setStatus(Level.TRACE, "Fetched " + result);
                } else {
                    process.increment(FetcherCounters.URLS_FAILED, 1);
                    process.setStatus(Level.ERROR, "Failed to fetch " + result);
                }
                
                // Cascading _collectors aren't thread-safe.
                TupleEntryCollector collector = _items.getCollector();
                synchronized (collector) {
                    collector.add(result.toTuple());
                }
            } catch (Throwable t) {
                if (fetching) {
                    process.decrement(FetcherCounters.URLS_FETCHING, 1);
                }
                
                process.increment(FetcherCounters.URLS_FAILED, 1);
                process.setStatus("Failed to fetch " + item, t);
            }
        }

        // All done fetching these items, so we're no longer hitting this
        // domain.
        
        _items.finished();
    }

}
