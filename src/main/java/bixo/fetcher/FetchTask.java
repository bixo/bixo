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
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.AbortedFetchException;
import bixo.exceptions.AbortedFetchReason;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.IOFetchException;
import bixo.hadoop.FetchCounters;
import cascading.tuple.Tuple;

import com.bixolabs.cascading.LoggingFlowProcess;

/**
 * Runnable instance for fetching a set of URLs from the same server, using keep-alive.
 *
 */
public class FetchTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(FetchTask.class);
    
    private IFetchMgr _fetchMgr;
    private BaseFetcher _httpFetcher;
    private List<ScoredUrlDatum> _items;
    private String _ref;
    
    public FetchTask(IFetchMgr fetchMgr, BaseFetcher httpFetcher, List<ScoredUrlDatum> items, String ref) {
        _fetchMgr = fetchMgr;
        _httpFetcher = httpFetcher;
        _items = items;
        _ref = ref;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        LoggingFlowProcess process = _fetchMgr.getProcess();
        process.increment(FetchCounters.DOMAINS_PROCESSING, 1);

        try {
            // TODO KKr - when fetching the last item, send a Connection: close
            // header to let the server know it doesn't need to keep the socket open.
            Iterator<ScoredUrlDatum> iter = _items.iterator();
            while (!Thread.interrupted() && iter.hasNext()) {
                ScoredUrlDatum item = iter.next();
                FetchedDatum result = new FetchedDatum(item);
                Comparable status = null;

                try {
                    process.increment(FetchCounters.URLS_FETCHING, 1);
                    long startTime = System.currentTimeMillis();
                    result = _httpFetcher.get(item);
                    long deltaTime = System.currentTimeMillis() - startTime;

                    process.increment(FetchCounters.FETCHED_TIME, (int)deltaTime);
                    process.increment(FetchCounters.URLS_FETCHED, 1);
                    process.increment(FetchCounters.FETCHED_BYTES, result.getContentLength());
                    process.setStatus(Level.TRACE, "Fetched " + result);

                    status = UrlStatus.FETCHED.toString();
                } catch (BaseFetchException e) {
                    // TODO KKr - we'd have to do something special here for AbortedFetchException with
                    // the reason == INTERRUPTED, as we'd want to (a) increment URLS_SKIPPED, not failed,
                    // and we'd want to bail out of this loop (or set the interrupted flag)
                    process.increment(FetchCounters.URLS_FAILED, 1);

                    // We can do this because each of the concrete subclasses of BaseFetchException implements
                    // WritableComparable
                    status = (Comparable)e;
                } catch (Exception e) {
                    LOGGER.warn("Unexpected exception while fetching " + item.getUrl(), e);

                    process.increment(FetchCounters.URLS_FAILED, 1);
                    status = new IOFetchException(item.getUrl(), new IOException(e));
                } finally {
                    process.decrement(FetchCounters.URLS_FETCHING, 1);

                    Tuple tuple = result.getTuple();
                    tuple.add(status);
                   _fetchMgr.collect(tuple);
                }
            }
            
            // While we still have entries, we need to write them out to avoid losing them.
            while (iter.hasNext()) {
                ScoredUrlDatum item = iter.next();
                FetchedDatum result = new FetchedDatum(item);
                process.increment(FetchCounters.URLS_SKIPPED, 1);
                AbortedFetchException status = new AbortedFetchException(item.getUrl(), AbortedFetchReason.INTERRUPTED);
                
                Tuple tuple = result.getTuple();
                tuple.add(status);
               _fetchMgr.collect(tuple);
            }
        } catch (Throwable t) {
            LOGGER.error("Exception while fetching", t);
        } finally {
            _fetchMgr.finished(_ref);
            process.decrement(FetchCounters.DOMAINS_PROCESSING, 1);
        }
    }

}
