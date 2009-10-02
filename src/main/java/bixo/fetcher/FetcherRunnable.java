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

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.IOFetchException;
import bixo.fetcher.http.IHttpFetcher;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FetcherRunnable implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(FetcherRunnable.class);
    
    private IHttpFetcher _httpFetcher;
    private FetchList _items;

    public FetcherRunnable(IHttpFetcher httpFetcher, FetchList items) {
        _httpFetcher = httpFetcher;
        _items = items;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        BixoFlowProcess process = _items.getProcess();
        TupleEntryCollector collector = _items.getCollector();

        // TODO KKr - when fetching the last item, send a Connection: close
        // header to let the server know it doesn't need to keep the socket open.
        for (ScoredUrlDatum item : _items) {
            FetchedDatum result = new FetchedDatum(item);
            Comparable status = null;
            
            try {
                process.increment(FetcherCounters.URLS_FETCHING, 1);
                long startTime = System.currentTimeMillis();
                result = _httpFetcher.get(item);
                long deltaTime = System.currentTimeMillis() - startTime;

                process.increment(FetcherCounters.FETCHED_TIME, (int)deltaTime);
                process.increment(FetcherCounters.URLS_FETCHED, 1);
                process.increment(FetcherCounters.FETCHED_BYTES, result.getContent().getLength());
                process.setStatus(Level.TRACE, "Fetched " + result);

                status = UrlStatus.FETCHED.toString();
            } catch (BaseFetchException e) {
                process.increment(FetcherCounters.URLS_FAILED, 1);
                
                // We can do this because each of the concrete subclasses of BaseFetchException implements
                // WritableComparable
                status = (Comparable)e;
            } catch (Exception e) {
                LOGGER.error("Expected exception while fetching " + item.getUrl(), e);
                
                process.increment(FetcherCounters.URLS_FAILED, 1);
                status = new IOFetchException(item.getUrl(), new IOException(e));
            } finally {
                process.decrement(FetcherCounters.URLS_FETCHING, 1);
                
                // Cascading _collectors aren't thread-safe.
                synchronized (collector) {
                    Tuple tuple = result.toTuple();
                    tuple.add(status);
                    collector.add(tuple);
                }
            }
        }

        // All done fetching these items, so we're no longer hitting this
        // domain.
        
        _items.finished();
    }

}
