package bixo.operations;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

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
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.BaseDatum;
import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;

@SuppressWarnings( { "serial", "unchecked" })
public class FetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext>, IFetchMgr {
    private static Logger LOGGER = Logger.getLogger(FetchBuffer.class);

    private class QueuedValues {
        private static final int MAX_ELEMENTS_IN_MEMORY = 1000;
        
        private DiskQueue<FetchSetDatum> _queue;
        private Iterator<TupleEntry> _values;
        private boolean _iteratorDone;
        
        public QueuedValues(Iterator<TupleEntry> values) {
            _values = values;
            _iteratorDone = false;
            _queue = new DiskQueue<FetchSetDatum>(MAX_ELEMENTS_IN_MEMORY);
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
        
        public FetchSetDatum nextOrNull(FetcherMode mode) {
            
            // Loop until we have something to return, or there's nothing that we can return.
            while (true) {
                // First see if we've got something in the queue, and if so, then check if it's ready
                // to be processed.
                FetchSetDatum datum = _queue.peek();
                if (datum != null) {
                    String ref = datum.getGroupingRef();
                    if (_activeRefs.get(ref) == null) {
                        Long nextFetchTime = _pendingRefs.get(ref);
                        if ((nextFetchTime == null) || (nextFetchTime <= System.currentTimeMillis())) {
                            return _queue.remove();
                        }
                    }
                }

                // We have a datum from the queue, but it's not ready to be returned.
                if (datum != null) {
                    switch (mode) {
                        case COMPLETE:
                            trace("Ignoring top queue item %s (domain still active or pending)", datum.getGroupingRef());
                            break;

                        case IMPOLITE:
                            _queue.remove();
                            return datum;
                            
                        // In efficient fetching, we punt on items that aren't ready.
                        case EFFICIENT:
                            _queue.remove();
                            List<ScoredUrlDatum> urls = datum.getUrls();
                            trace("Skipping %d urls from %s (e.g. %s)", urls.size(), datum.getGroupingRef(), urls.get(0).getUrl());
                            skipUrls(urls, UrlStatus.SKIPPED_INEFFICIENT, null);
                            break;
                    }
                }
                
                // Nothing ready in the queue, let's see about the iterator.
                if (safeHasNext()) {
                    datum = new FetchSetDatum(new TupleEntry(_values.next()));
                    if (datum.isSkipped()) {
                        List<ScoredUrlDatum> urls = datum.getUrls();
                        trace("Skipping %d urls from %s (e.g. %s)", urls.size(), datum.getGroupingRef(), urls.get(0).getUrl());
                        skipUrls(urls, UrlStatus.SKIPPED_PER_SERVER_LIMIT, null);
                        continue;
                    }
                    
                    String ref = datum.getGroupingRef();
                    if (_activeRefs.get(ref) == null) {
                        Long nextFetchTime = _pendingRefs.get(ref);
                        if ((nextFetchTime == null) || (nextFetchTime <= System.currentTimeMillis())) {
                            return datum;
                        }
                    }

                    if (datum != null) {
                        switch (mode) {
                            case COMPLETE:
                                trace("Queuing next iter item %s (domain still active or pending)", datum.getGroupingRef());
                                _queue.add(datum);
                                break;

                            case IMPOLITE:
                                return datum;
                                
                            // In efficient fetching, we punt on items that aren't ready.
                            case EFFICIENT:
                                List<ScoredUrlDatum> urls = datum.getUrls();
                                trace("Skipping %d urls from %s (e.g. %s)", urls.size(), datum.getGroupingRef(), urls.get(0).getUrl());
                                skipUrls(urls, UrlStatus.SKIPPED_INEFFICIENT, null);
                                break;
                        }
                    }
                } else {
                    // TODO KKr - have a peek(index) and a remove(index) call for the DiskQueue,
                    // where index < number of elements in memory. That way we don't get stuck on having
                    // a top-most element that's taking a long time, but there are following elements that
                    // would be ready to go. Or we could just make sure that the DiskQueue orders
                    // elements by a comparator (for the in memory set), so top-most is always the
                    // one that's closest to being ready (or biggest, if more than one is ready)
                    return null;
                }
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

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        super.prepare(flowProcess, operationCall);

        _flowProcess = new LoggingFlowProcess((HadoopFlowProcess) flowProcess);
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
                FetchSetDatum datum = values.nextOrNull(FetcherMode.IMPOLITE);
                List<ScoredUrlDatum> urls = datum.getUrls();
                trace("Skipping %d urls from %s (e.g. %s) ", urls.size(), datum.getGroupingRef(), urls.get(0).getUrl());
                skipUrls(datum.getUrls(), status, null);
            }
        }
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall operationCall) {
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
        }

        _flowProcess.dumpCounters();
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
                _collector.add(tuple);
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
            _collector.add(tuple);
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
