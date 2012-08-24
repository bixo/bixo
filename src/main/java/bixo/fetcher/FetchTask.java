/*
 * Copyright 2009-2012 Scale Unlimited
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

    // Min duration (in milliseconds) between page fetches in a single fetch set.
    private static final long MIN_PAGE_FETCH_INTERVAL = 1000L;
    
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

    @SuppressWarnings("rawtypes")
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
                
                // We use status as an extra field on the end of of FetchedDatum that lets
                // us generate a full status pipe, and also a content pipe that only has
                // entries which were fetched. By keying off the type (string == OK,
                // BaseFetchException == bad) the FetchPipe can do this magic.
                Comparable status = null;

                long fetchStartTime = System.currentTimeMillis();
                
                try {
                    process.increment(FetchCounters.URLS_FETCHING, 1);
                    result = _httpFetcher.get(item);
                    long deltaTime = System.currentTimeMillis() - fetchStartTime;

                    process.increment(FetchCounters.FETCHED_TIME, (int)deltaTime);
                    process.increment(FetchCounters.URLS_FETCHED, 1);
                    process.increment(FetchCounters.FETCHED_BYTES, result.getContentLength());
                    process.setStatus(Level.TRACE, "Fetched " + result);

                    status = UrlStatus.FETCHED.toString();
                    
                    // TODO - check keep-alive response (if present), and close the connection/delay
                    // for some amount of time if we exceed this limit.
                } catch (BaseFetchException e) {
                    // TODO KKr - we'd have to do something special here for AbortedFetchException with
                    // the reason == INTERRUPTED, as we'd want to (a) increment URLS_SKIPPED, not failed,
                    // and we'd want to bail out of this loop (or set the interrupted flag)
                    LOGGER.info("Fetch exception while fetching " + item.getUrl(), e);
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
                   
                   // Figure out how long it's been since the start of the request.
                   long fetchInterval = System.currentTimeMillis() - fetchStartTime;
                   long delay = Math.min(0, MIN_PAGE_FETCH_INTERVAL - fetchInterval);
                   
                   // We want to avoid fetching faster than a max acceptable rate.
                   if (delay > 0) {
                       LOGGER.trace(String.format("FetchTask: sleeping for %dms", delay));
                       
                       try {
                           Thread.sleep(delay);
                       } catch (InterruptedException e) {
                           LOGGER.warn("FetchTask interrupted!");
                           Thread.currentThread().interrupt();
                           continue;
                       }
                   }
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
