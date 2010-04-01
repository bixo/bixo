package bixo.operations;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.cascading.NullContext;
import bixo.datum.BaseDatum;
import bixo.datum.FetchedDatum;
import bixo.datum.PreFetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.FetchTask;
import bixo.fetcher.IFetchMgr;
import bixo.fetcher.http.IHttpFetcher;
import bixo.hadoop.FetchCounters;
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

@SuppressWarnings( { "serial", "unchecked" })
public class FetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext>, IFetchMgr {
    private static Logger LOGGER = Logger.getLogger(FetchBuffer.class);

    private static final Fields FETCH_RESULT_FIELD = new Fields(BaseDatum.fieldName(FetchBuffer.class, "fetch-exception"));

    // How long to wait before a fetch request gets rejected.
    // TODO KKr - calculate this based on the fetcher policy's max URLs/request
    private static final long REQUEST_TIMEOUT = 100 * 1000L;
    
    // How long to wait before doing a hard termination. Wait twice as long
    // as the longest request.
    private static final long TERMINATION_TIMEOUT = REQUEST_TIMEOUT * 2;

    private IHttpFetcher _fetcher;
    private long _crawlEndTime;
    private final Fields _metaDataFields;

    private transient ThreadedExecutor _executor;
    private transient BixoFlowProcess _flowProcess;
    private transient TupleEntryCollector _collector;

    private transient Object _refLock;
    private transient ConcurrentHashMap<String, Long> _activeRefs;
    private transient ConcurrentHashMap<String, Long> _pendingRefs;

    public FetchBuffer(IHttpFetcher fetcher, long crawlEndTime, Fields metaDataFields) {
        // We're going to output a tuple that contains a FetchedDatum, plus meta-data,
        // plus a result that could be a string, a status, or an exception
        super(FetchedDatum.FIELDS.append(metaDataFields).append(FETCH_RESULT_FIELD));

        _fetcher = fetcher;
        _crawlEndTime = crawlEndTime;
        _metaDataFields = metaDataFields;
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

        // FUTURE KKr - use Cascading process vs creating our own, once it
        // supports logging in local mode, and a setStatus() call.
        // FUTURE KKr - check for a serialized external reporter in the process,
        // add it if it exists.
        _flowProcess = new BixoFlowProcess((HadoopFlowProcess) flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());

        _executor = new ThreadedExecutor(_fetcher.getMaxThreads(), REQUEST_TIMEOUT);

        _refLock = new Object();
        _pendingRefs = new ConcurrentHashMap<String, Long>();
        _activeRefs = new ConcurrentHashMap<String, Long>();
    }

    @Override
    public void operate(FlowProcess process, BufferCall<NullContext> buffCall) {
        Iterator<TupleEntry> values = buffCall.getArgumentsIterator();
        _collector = buffCall.getOutputCollector();

        // Each value is a PreFetchedDatum that contains a set of URLs to fetch in one request from
        // a single server, plus other values needed to set state properly.
        while (!Thread.interrupted() && (System.currentTimeMillis() < _crawlEndTime) && values.hasNext()) {
            PreFetchedDatum datum = new PreFetchedDatum(values.next().getTuple(), _metaDataFields);
            List<ScoredUrlDatum> urls = datum.getUrls();
            String ref = datum.getGroupingKey().getRef();
            trace("Processing %d URLs for %s", urls.size(), ref);
            
            try {
                checkFetchTime(ref, _activeRefs.get(ref));
                checkFetchTime(ref, _pendingRefs.get(ref));

                Long nextFetchTime;
                
                // Figure out the correct time for the fetch after this one.
                if (datum.isLastList()) {
                    nextFetchTime = 0L;
                } else {
                    nextFetchTime = System.currentTimeMillis() + datum.getFetchDelay();
                }

                Runnable doFetch = new FetchTask(this, _fetcher, urls, ref);
                makeActive(ref, nextFetchTime);
                trace("Executing batch fetch %s to %d", ref, nextFetchTime);
                long startTime = System.currentTimeMillis();
                _executor.execute(doFetch);
                
                // Adjust for how long it took to get the request queued.
                adjustActive(ref, System.currentTimeMillis() - startTime);
                
                _flowProcess.increment(FetchCounters.URLS_QUEUED, urls.size());
                _flowProcess.increment(FetchCounters.URLS_REMAINING, urls.size());
            } catch (RejectedExecutionException e) {
                // should never happen.
                LOGGER.error("Fetch pool rejected our fetch list for " + ref);
                
                finished(ref);
                
                _flowProcess.increment(FetchCounters.URLS_SKIPPED, urls.size());
                _flowProcess.decrement(FetchCounters.URLS_REMAINING, urls.size());
                
                skipUrls(urls, UrlStatus.SKIPPED_DEFERRED, String.format("Execution rejection skipped %d URLs", urls.size()));
            } catch (InterruptedException e) {
                LOGGER.warn("FetchBuffer interrupted!");
                Thread.currentThread().interrupt();
                
                // TODO KKr - use different key for interrupted case
                skipUrls(urls, UrlStatus.SKIPPED_DEFERRED, null);
            }
        }
        
        // Skip all URLs that we've got left.
        if (values.hasNext()) {
            trace("Found unprocessed URLs");
            
            // TODO KKr - use different key for interrupted case
            UrlStatus status = Thread.interrupted() ? UrlStatus.SKIPPED_DEFERRED : UrlStatus.SKIPPED_TIME_LIMIT;
            
            while (values.hasNext()) {
                PreFetchedDatum datum = new PreFetchedDatum(values.next().getTuple(), _metaDataFields);
                List<ScoredUrlDatum> urls = datum.getUrls();
                trace("Skipping %d urls from %s", urls.size(), datum.getGroupingKey().getRef());
                skipUrls(datum.getUrls(), status, null);
            }
        }
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall operationCall) {
        try {
            if (!_executor.terminate(TERMINATION_TIMEOUT)) {
                LOGGER.warn("Had to do a hard termination of general fetching");
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
            Long nextFetchTime = _activeRefs.get(ref);
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
    public TupleEntryCollector getCollector() {
        return _collector;
    }

    @Override
    public BixoFlowProcess getProcess() {
        return _flowProcess;
    }
    
    private void skipUrls(List<ScoredUrlDatum> urls, UrlStatus status, String traceMsg) {
        for (ScoredUrlDatum datum : urls) {
            FetchedDatum result = new FetchedDatum(datum);
            Tuple tuple = result.toTuple();
            tuple.add(status.toString());
            _collector.add(tuple);
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

    private void checkFetchTime(String ref, Long nextFetchTime) throws InterruptedException {
        if ((nextFetchTime != null) && (nextFetchTime > System.currentTimeMillis())) {
            // We have to wait until it's time to fetch.
            long delta = nextFetchTime - System.currentTimeMillis();
            trace("Waiting %dms until we can fetch from %s", delta, ref);
            Thread.sleep(delta);
        }
    }
    

}
