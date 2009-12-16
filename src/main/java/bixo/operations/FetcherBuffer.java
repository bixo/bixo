package bixo.operations;

import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.datum.BaseDatum;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.FetcherManager;
import bixo.fetcher.FetcherQueue;
import bixo.fetcher.FetcherQueueMgr;
import bixo.fetcher.http.IHttpFetcher;
import bixo.hadoop.FetchCounters;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings( { "serial", "unchecked" })
public class FetcherBuffer extends BaseOperation implements cascading.operation.Buffer {
    private static Logger LOGGER = Logger.getLogger(FetcherBuffer.class);

    private static final Fields FETCH_RESULT_FIELD = new Fields(BaseDatum.fieldName(FetcherBuffer.class, "fetch-exception"));
    
    private FetcherManager _fetcherMgr;
    private FetcherQueueMgr _queueMgr;
    private Thread _fetcherThread;
    private BixoFlowProcess _flowProcess;
    private IHttpFetcher _fetcher;

    private final Fields _metaDataFields;

    public FetcherBuffer(Fields metaDataFields, IHttpFetcher fetcher) {
        // We're going to output a tuple that contains a FetchedDatum, plus meta-data,
        // plus a result that could be a string, a status, or an exception
        super(FetchedDatum.FIELDS.append(metaDataFields).append(FETCH_RESULT_FIELD));

        _metaDataFields = metaDataFields;
        _fetcher = fetcher;
    }

    @Override
    public boolean isSafe() {
        // We definitely DO NOT want to be called multiple times for the same
        // scored datum, so let Cascading know that the output from us should
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
        
        _queueMgr = new FetcherQueueMgr(_flowProcess, _fetcher.getFetcherPolicy());
        _fetcherMgr = new FetcherManager(_queueMgr, _fetcher, _flowProcess);

        _fetcherThread = new Thread(_fetcherMgr);
        _fetcherThread.setName("Fetcher manager");
        _fetcherThread.start();
    }

    @Override
    public void operate(FlowProcess process, BufferCall buffCall) {
        Iterator<TupleEntry> values = buffCall.getArgumentsIterator();
        TupleEntry group = buffCall.getGroup();

        try {
            // <key> is the output of the IGroupingKeyGenerator used. This should
            // be either one of the special values (for URLs that shouldn't be fetched),
            // as defined via static values in GroupingKey, or it will be
            // <key>-<crawl delay in ms>
            String key = group.getString(0);

            if (GroupingKey.isSpecialKey(key)) {
                emptyBuffer(key, values, buffCall.getOutputCollector());
            } else {
                String domain = GroupingKey.getDomainFromKey(key);
                long crawlDelay = GroupingKey.getCrawlDelayFromKey(key);
                TupleEntryCollector collector = buffCall.getOutputCollector();
                FetcherQueue queue = _queueMgr.createQueue(domain, collector, crawlDelay);

                while (values.hasNext()) {
                    Tuple curTuple = values.next().getTuple();
                    ScoredUrlDatum scoreUrl = new ScoredUrlDatum(curTuple, _metaDataFields);
                    queue.offer(scoreUrl);
                }

                _flowProcess.increment(FetchCounters.URLS_QUEUED, queue.getNumQueued());
                _flowProcess.increment(FetchCounters.URLS_REMAINING, queue.getNumQueued());
                _flowProcess.increment(FetchCounters.URLS_SKIPPED, queue.getNumSkipped());

                // We're going to spin here until the queue manager decides that we
                // have available space for this next queue.
                // TODO KKr - have timeout here based on target fetch duration.
                while (!_queueMgr.offer(queue)) {
                    process.keepAlive();
                }

                LOGGER.info(String.format("Queued %d URLs from %s", queue.getNumQueued(), domain));
                LOGGER.info(String.format("Skipped %d URLs from %s", queue.getNumSkipped(), domain));

                _flowProcess.increment(FetchCounters.DOMAINS_QUEUED, 1);
                _flowProcess.increment(FetchCounters.DOMAINS_REMAINING, 1);
            }

        } catch (Throwable t) {
            LOGGER.error("Exception during creating of fetcher queues", t);
        }

    }

    @Override
    public void cleanup(FlowProcess process, OperationCall operationCall) {
        try {
            while (!_fetcherMgr.isDone()) {
                process.keepAlive();

                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                }

            }

            // TODO KKr - this need to interrupt the fetcher thread feels awkward.
            // I'd rather have
            // a FetcherManager.create() factory method that returns a FetcherMgr,
            // but also starts
            // up the thread. Not sure how to terminate the thread in that case,
            // unless I no flip
            // things around and don't make the FetchManager be runnable, but rather
            // have a run
            // method that spawns a thread and immediately returns.
            _fetcherThread.interrupt();

            // TODO KKr - shut down FetcherManager, so that it can do...
            // httpclient.getConnectionManager().shutdown();

            // Write out counter info we've collected, in case we're running in
            // local mode.
            _flowProcess.dumpCounters();
        } catch (Throwable t) {
            // If we run into a serious error, just log it and return, so that we
            // don't lose the entire fetch result.
            LOGGER.error("Error during cleanup of FetcherBuffer", t);
        }
    }

    private Tuple makeFetchedTuple(ScoredUrlDatum scoredUrl, UrlStatus status) {
        FetchedDatum result = new FetchedDatum(scoredUrl);
        Tuple tuple = result.toTuple();
        tuple.add(status.toString());
        return tuple;
    }

    private void emptyBuffer(String key, Iterator<TupleEntry> values, TupleEntryCollector collector) {
        String traceMsg = null;
        UrlStatus status;
        
        if (key.equals(GroupingKey.BLOCKED_GROUPING_KEY)) {
            traceMsg = "Blocked %d URLs";
            status = UrlStatus.SKIPPED_BLOCKED;
        } else if (key.equals(GroupingKey.UNKNOWN_HOST_GROUPING_KEY)) {
            traceMsg = "Host not found for %d URLs";
            status = UrlStatus.SKIPPED_UNKNOWN_HOST;
        } else if (key.equals(GroupingKey.INVALID_URL_GROUPING_KEY)) {
            traceMsg = "Invalid format for %d URLs";
            status = UrlStatus.SKIPPED_INVALID_URL;
        } else if (key.equals(GroupingKey.DEFERRED_GROUPING_KEY)) {
            traceMsg = "Robots.txt problems deferred processing of %d URLs";
            status = UrlStatus.SKIPPED_DEFERRED;
        } else if (key.equals(GroupingKey.SKIPPED_GROUPING_KEY)) {
            traceMsg = "Scoring explicitly skipping %d URLs";
            status = UrlStatus.SKIPPED_BY_SCORER;
        } else {
            throw new RuntimeException("Unknown value for special grouping key: " + key);
        }

        int numUrls = 0;
        while (values.hasNext()) {
            ScoredUrlDatum scoredDatum = new ScoredUrlDatum(values.next().getTuple(), _metaDataFields);
            Tuple tuple = makeFetchedTuple(scoredDatum, status);
            collector.add(tuple);

            numUrls += 1;
        }
        
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format(traceMsg, numUrls));
        }
        
    }
}
