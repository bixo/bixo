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
package bixo.operations;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.datum.FetchSetDatum;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.FetchTask;
import bixo.fetcher.IFetchMgr;
import bixo.hadoop.FetchCounters;
import bixo.utils.DiskQueue;
import bixo.utils.ThreadedExecutor;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BaseDatum;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;


@SuppressWarnings( { "serial", "rawtypes" })
public class FetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext>, IFetchMgr {
    private static Logger LOGGER = LoggerFactory.getLogger(FetchBuffer.class);

    private class QueuedFetchSetsComparator implements Comparator<FetchSetDatum> {

        private long getFetchTime(String groupingRef) {
            if (_activeRefs.get(groupingRef) == null) {
                Long nextFetchTime = _pendingRefs.get(groupingRef);
                if (nextFetchTime == null) {
                    return(0);
                } else {
                    return(nextFetchTime);
                }
            } else {
                // fetch set is active, so sort at end
                return(Long.MAX_VALUE);
            }
        }
        
        @Override
        public int compare(FetchSetDatum o1, FetchSetDatum o2) {
            long o1FetchTime = getFetchTime(o1.getGroupingRef());
            long o2FetchTime = getFetchTime(o2.getGroupingRef());
            
            // The entry that's ready sooner sorts sooner. If both
            // are ready, return the one with the bigger fetch set.
            if (o1FetchTime < o2FetchTime) {
                return -1;
            } else if (o1FetchTime > o2FetchTime) {
                return 1;
            } else if (o1.getUrls().size() > o2.getUrls().size()) {
                return -1;
            } else if (o1.getUrls().size() < o2.getUrls().size()) {
                return 1;
            } else {
                return 0;
            }
        }
        
    }
    
    private class QueuedValues {
        // TODO - make this part of CrawlPolicy. We'd like to contrain by total # of URLs, actually, not FetchSetDatums
        private static final int MAX_ELEMENTS_IN_MEMORY = 10000;
        
        private static final int MAX_FETCHSETS_TO_QUEUE_PER_DELAY = 100;
        
        private DiskQueue<FetchSetDatum> _queue;
        private Iterator<TupleEntry> _values;
        private boolean _iteratorDone;
        
        public QueuedValues(Iterator<TupleEntry> values) {
            _values = values;
            _iteratorDone = false;
            _queue = new DiskQueue<FetchSetDatum>(MAX_ELEMENTS_IN_MEMORY, new QueuedFetchSetsComparator());
        }
        
        /**
         * Return true if the iterator has another Tuple. This avoids calling
         * the hasNext() method after it returns false, as doing so with
         * Cascading 1.2 will trigger a NPE.
         * 
         * @return true if there's another Tuple waiting to be read.
         */
        private boolean safeHasNext() {
            _iteratorDone = _iteratorDone || !_values.hasNext();
            return !_iteratorDone;
        }
        
        public boolean isEmpty() {
            return _queue.isEmpty() && !safeHasNext();
        }
        
        private boolean readyToFetch(String ref) {
            if (_activeRefs.get(ref) == null) {
                Long nextFetchTime = _pendingRefs.get(ref);
                if ((nextFetchTime == null) || (nextFetchTime <= System.currentTimeMillis())) {
                    return true;
                }
            }
            
            return false;
        }
        
        public FetchSetDatum nextOrNull(FetcherMode mode) {
            
            int fetchSetsQueued = 0;
            
            // Loop until we have something to return, or there's nothing that we can return, or we've
            // queued up as many fetchsets as we want without any delay.
            while (!isEmpty() && (fetchSetsQueued < MAX_FETCHSETS_TO_QUEUE_PER_DELAY)) {
                // First see if we've got something in the queue, and if so, then check if it's ready
                // to be processed.
                final FetchSetDatum queueDatum = removeFromQueue();
                
                if (queueDatum != null) {
                    String ref = queueDatum.getGroupingRef();
                    if (readyToFetch(ref) || (mode == FetcherMode.IMPOLITE)) {
                        List<ScoredUrlDatum> urls = queueDatum.getUrls();
                        trace("Returning %d urls via queue from %s (e.g. %s)", urls.size(), ref, urls.get(0).getUrl());
                        return queueDatum;
                    }
                }

                // Nothing ready from the top of the queue or nothing in the queue, let's see about the iterator.
                if (safeHasNext()) {
                    // Re-add the thing from the top of the queue, since we're going to want to keep it around.
                    // This is safe to call with a null datum.
                    addToQueue(queueDatum);
                    
                    // Now get our next FetchSet from the Hadoop iterator.
                    FetchSetDatum iterDatum = new FetchSetDatum(new TupleEntry(_values.next()));
                    List<ScoredUrlDatum> urls = iterDatum.getUrls();
                    String ref = iterDatum.getGroupingRef();
                    
                    if (iterDatum.isSkipped()) {
                        trace("Skipping %d urls via iterator from %s (e.g. %s)", urls.size(), ref, urls.get(0).getUrl());
                        skipUrls(urls, UrlStatus.SKIPPED_PER_SERVER_LIMIT, null);
                        continue;
                    }

                    if ((mode == FetcherMode.IMPOLITE) || readyToFetch(ref)) {
                        trace("Returning %d urls via iterator from %s (e.g. %s)", urls.size(), ref, urls.get(0).getUrl());
                        return iterDatum;
                    }

                    // We've got a datum from the iterator that's not ready to be processed, so we'll stuff it into the queue.
                    trace("Queuing %d urls via iterator from %s (e.g. %s)", urls.size(), iterDatum.getGroupingRef(), urls.get(0).getUrl());
                    addToQueue(iterDatum);
                    fetchSetsQueued += 1;
                    continue;
                }
                
                // Nothing ready from top of queue, and iterator is empty too. If we had something from the top of the queue (which then
                // must not be ready), decide what to do based on our FetcherMode.
                if (queueDatum != null) {
                    List<ScoredUrlDatum> urls = queueDatum.getUrls();

                    switch (mode) {
                        case COMPLETE:
                            // Re-add the datum, since we don't want to skip it. And immediately return, as otherwise we're trapped
                            // in this loop, versus giving FetchBuffer time to delay.
                            trace("Blocked on %d urls via queue from %s (e.g. %s)", urls.size(), queueDatum.getGroupingRef(), urls.get(0).getUrl());
                            addToQueue(queueDatum);
                            return null;

                        case IMPOLITE:
                            trace("Impolitely returning %d urls via queue from %s (e.g. %s)", urls.size(), queueDatum.getGroupingRef(), urls.get(0).getUrl());
                            return queueDatum;
                            
                        case EFFICIENT:
                            // In efficient fetching, we punt on items that aren't ready. And immediately return, so that FetchBuffer's loop has
                            // time to delay, as otherwise we'd likely skip everything that's in the in-memory queue (since the item we're skipping
                            // is the "best" in terms of when it's going to be ready).
                            trace("Efficiently skipping %d urls via queue from %s (e.g. %s)", urls.size(), queueDatum.getGroupingRef(), urls.get(0).getUrl());
                            skipUrls(urls, UrlStatus.SKIPPED_INEFFICIENT, null);
                            return null;
                    }
                }
            }
            
            // Either we're all out of FetchSets to process (nothing left in iterator or queue) or we've queued up lots of sets, and
            // we want to give FetchBuffer a chance to sleep.
            return null;
        }
        
        /**
         * Empty the buffer, then the iterator, without worrying about mode/state.
         * 
         * @return
         */
        public FetchSetDatum drain() {
            if (!_queue.isEmpty()) {
                return removeFromQueue();
            } else if (safeHasNext()) {
                return new FetchSetDatum(new TupleEntry(_values.next()));
            } else {
                return null;
            }
        }

        /**
         * Return the top-most item from the queue, or null if the queue is empty.
         * 
         * @return fetch set from queue
         */
        private FetchSetDatum removeFromQueue() {
            FetchSetDatum result = _queue.poll();
            if (result != null) {
                _flowProcess.increment(FetchCounters.FETCHSETS_QUEUED, -1);
                _flowProcess.increment(FetchCounters.URLS_QUEUED, -result.getUrls().size());
            }
            
            return result;
        }

        private void addToQueue(FetchSetDatum datum) {
            if (datum != null) {
                _flowProcess.increment(FetchCounters.FETCHSETS_QUEUED, 1);
                _flowProcess.increment(FetchCounters.URLS_QUEUED, datum.getUrls().size());

                _queue.add(datum);
            }
        }
}

    private static final Fields FETCH_RESULT_FIELD = new Fields(BaseDatum.fieldName(FetchBuffer.class, "fetch-exception"));

    // Time to sleep when we don't have any URLs that can be fetched.
    private static final long NOTHING_TO_FETCH_SLEEP_TIME = 1000;

    private static final long HARD_TERMINATION_CLEANUP_DURATION = 10 * 1000L;

    private BaseFetcher _fetcher;
    private FetcherMode _fetcherMode;

    private transient ThreadedExecutor _executor;
    private transient LoggingFlowProcess _flowProcess;
    private transient TupleEntryCollector _collector;

    private transient Object _refLock;
    private transient ConcurrentHashMap<String, Long> _activeRefs;
    private transient ConcurrentHashMap<String, Long> _pendingRefs;
    
    private transient AtomicBoolean _keepCollecting;
    
    public FetchBuffer(BaseFetcher fetcher) {
        // We're going to output a tuple that contains a FetchedDatum, plus meta-data,
        // plus a result that could be a string, a status, or an exception
        super(FetchedDatum.FIELDS.append(FETCH_RESULT_FIELD));

        _fetcher = fetcher;
        _fetcherMode = _fetcher.getFetcherPolicy().getFetcherMode();
    }

    @Override
    public boolean isSafe() {
        // We definitely DO NOT want to be called multiple times for the same
        // scored datum, so let Cascading 1.1 know that the output from us should
        // be stashed in tempHfs if need be.
        return false;
    }

    @SuppressWarnings({"unchecked" })
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);

        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());

        _executor = new ThreadedExecutor(_fetcher.getMaxThreads(), _fetcher.getFetcherPolicy().getRequestTimeout());

        _refLock = new Object();
        _pendingRefs = new ConcurrentHashMap<String, Long>();
        _activeRefs = new ConcurrentHashMap<String, Long>();
        
        _keepCollecting = new AtomicBoolean(true);
    }

    @Override
    public void operate(FlowProcess process, BufferCall<NullContext> buffCall) {
        QueuedValues values = new QueuedValues(buffCall.getArgumentsIterator());

        _collector = buffCall.getOutputCollector();
        FetcherPolicy fetcherPolicy = _fetcher.getFetcherPolicy();
        
        // Each value is a PreFetchedDatum that contains a set of URLs to fetch in one request from
        // a single server, plus other values needed to set state properly.
        while (!Thread.interrupted() && !fetcherPolicy.isTerminateFetch() && !values.isEmpty()) {
            FetchSetDatum datum = values.nextOrNull(_fetcherMode);
            
            try {
                if (datum == null) {
                    trace("Nothing ready to fetch, sleeping...");
                    process.keepAlive();
                    Thread.sleep(NOTHING_TO_FETCH_SLEEP_TIME);
                } else {
                    List<ScoredUrlDatum> urls = datum.getUrls();
                    String ref = datum.getGroupingRef();
                    trace("Processing %d URLs for %s", urls.size(), ref);

                    Runnable doFetch = new FetchTask(this, _fetcher, urls, ref);
                    if (datum.isLastList()) {
                        makeActive(ref, 0L);
                        trace("Executing fetch of %d URLs from %s (last batch)", urls.size(), ref);
                    } else {
                        Long nextFetchTime = System.currentTimeMillis() + datum.getFetchDelay();
                        makeActive(ref, nextFetchTime);
                        trace("Executing fetch of %d URLs from %s (next fetch time %d)", urls.size(), ref, nextFetchTime);
                    }

                    long startTime = System.currentTimeMillis();

                    try {
                        _executor.execute(doFetch);
                    } catch (RejectedExecutionException e) {
                        // should never happen.
                        LOGGER.error("Fetch pool rejected our fetch list for " + ref);

                        finished(ref);
                        skipUrls(urls, UrlStatus.SKIPPED_DEFERRED, String.format("Execution rejection skipped %d URLs", urls.size()));
                    }

                    // Adjust for how long it took to get the request queued.
                    adjustActive(ref, System.currentTimeMillis() - startTime);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("FetchBuffer interrupted!");
                Thread.currentThread().interrupt();
            }
        }
        
        // Skip all URLs that we've got left.
        if (!values.isEmpty()) {
            trace("Found unprocessed URLs");
            
            UrlStatus status = Thread.interrupted() ? UrlStatus.SKIPPED_INTERRUPTED : UrlStatus.SKIPPED_TIME_LIMIT;
            
            while (!values.isEmpty()) {
                FetchSetDatum datum = values.drain();
                List<ScoredUrlDatum> urls = datum.getUrls();
                trace("Skipping %d urls from %s (e.g. %s) ", urls.size(), datum.getGroupingRef(), urls.get(0).getUrl());
                skipUrls(urls, status, null);
            }
        }
    }

    private synchronized void terminate() {
        if (_executor == null) {
            return;
        }
        
        try {
            // We don't know worst-case for amount of time a worker thread will effectively
            // "sleep" waiting for a FetchTask to be queued up, but we'll add in a bit of
            // slop to represent that amount of time.
            long pollTime = ThreadedExecutor.MAX_POLL_TIME;
            Thread.sleep(pollTime);
            
            long requestTimeout = _fetcher.getFetcherPolicy().getRequestTimeout();
            if (!_executor.terminate(requestTimeout)) {
                LOGGER.warn("Had to do a hard termination of general fetching");
                
                // Abort any active connections, which should give the FetchTasks a chance
                // to clean things up.
                _fetcher.abort();
                
                // Now give everybody who had to be interrupted some time to
                // actually write out their remaining URLs.
                Thread.sleep(HARD_TERMINATION_CLEANUP_DURATION);
            }
            
            // Now stop collecting results. If somebody is in the middle of the collect() call,
            // we want them to finish before we set it to false and drop out of this method.
            synchronized (_keepCollecting) {
                _keepCollecting.set(false);
            }
        } catch (InterruptedException e) {
            // FUTURE What's the right thing to do here? E.g. do I need to worry about
            // losing URLs still to be processed?
            LOGGER.warn("Interrupted while waiting for termination");
        } finally {
            _executor = null;
        }
    }
    
    @Override
    public void flush(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Flushing FetchBuffer");
        
        terminate();

        super.flush(process, operationCall);
    }
    
    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Cleaning up FetchBuffer");
        
        // TODO - do we need the terminate here? Shouldn't, right?
        // terminate();

        _flowProcess.dumpCounters();
        super.cleanup(process,  operationCall);
    }

    @Override
    public void finished(String ref) {
        synchronized (_refLock) {
            Long nextFetchTime = _activeRefs.remove(ref);
            if (nextFetchTime == null) {
                throw new RuntimeException("finished called on non-active ref: " + ref);
            }
            
            // If there's going to be more to fetch, put it back in the pending pool.
            if (nextFetchTime != 0) {
                trace("Finished batch fetch for %s, with next batch at %d", ref, nextFetchTime);
                _pendingRefs.put(ref, nextFetchTime);
            } else {
                trace("Finished last batch fetch for %s", ref);
            }
        }
    }

    @Override
    public void collect(Tuple tuple) {
        // Prevent two bad things from happening:
        // 1. Somebody changes _keepCollecting after we've tested that it's true
        // 2. Two people calling collector.add() at the same time (it's not thread safe)
        synchronized (_keepCollecting) {
            if (_keepCollecting.get()) {
                _collector.add(BixoPlatform.clone(tuple, _flowProcess));
            } else {
                LOGGER.warn("Losing an entry: " + tuple);
            }
        }
    }

    @Override
    public LoggingFlowProcess getProcess() {
        return _flowProcess;
    }
    
    private void skipUrls(List<ScoredUrlDatum> urls, UrlStatus status, String traceMsg) {
        for (ScoredUrlDatum datum : urls) {
            FetchedDatum result = new FetchedDatum(datum);
            Tuple tuple = result.getTuple();
            tuple.add(status.toString());
            _collector.add(BixoPlatform.clone(tuple, _flowProcess));
        }

        _flowProcess.increment(FetchCounters.URLS_SKIPPED, urls.size());
        if (status == UrlStatus.SKIPPED_PER_SERVER_LIMIT) {
            _flowProcess.increment(FetchCounters.URLS_SKIPPED_PER_SERVER_LIMIT, urls.size());
        }

        if ((traceMsg != null) && LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format(traceMsg, urls.size()));
        }
    }
    
    /**
     * Make <ref> active, removing from pending if necessary.
     * 
     * @param ref
     * @param nextFetchTime
     */
    private void makeActive(String ref, Long nextFetchTime) {
        synchronized (_refLock) {
            trace("Making %s active", ref);
            _pendingRefs.remove(ref);
            _activeRefs.put(ref, nextFetchTime);
        }
    }

    private void adjustActive(String ref, long deltaTime) {
        synchronized (_refLock) {
            Long nextFetchTime = _activeRefs.get(ref);
            if ((nextFetchTime != null) && (nextFetchTime != 0) && (deltaTime != 0)) {
                _activeRefs.put(ref, nextFetchTime + deltaTime);
            }
        }
    }

    private void trace(String template, Object... params) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format(template, params));
        }
    }
    

}
