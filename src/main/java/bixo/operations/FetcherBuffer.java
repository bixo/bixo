package bixo.operations;

import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.config.FetcherPolicy;
import bixo.config.QueuePolicy;
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

    // Time to sleep while offering URLs to the queue manager.
    private static final long OFFER_QUEUE_DELAY = 10;
    
    private FetcherManager _fetcherMgr;
    private FetcherQueueMgr _queueMgr;
    private Thread _fetcherThread;
    private BixoFlowProcess _flowProcess;
    private IHttpFetcher _fetcher;
    private FetcherPolicy _fetcherPolicy;
    private QueuePolicy _queuePolicy;
    
    private final Fields _metaDataFields;

    public FetcherBuffer(Fields metaDataFields, IHttpFetcher fetcher, QueuePolicy queuePolicy) {
        // We're going to output a tuple that contains a FetchedDatum, plus meta-data,
        // plus a result that could be a string, a status, or an exception
        super(FetchedDatum.FIELDS.append(metaDataFields).append(FETCH_RESULT_FIELD));

        _metaDataFields = metaDataFields;
        _fetcher = fetcher;
        _fetcherPolicy = fetcher.getFetcherPolicy();
        _queuePolicy = queuePolicy;
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
        
        _queueMgr = new FetcherQueueMgr(_flowProcess, _fetcherPolicy, _queuePolicy);
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

                int numUrlsQueued = 0;
                int numUrlsSkipped = 0;
                while (values.hasNext()) {
                    Tuple curTuple = values.next().getTuple();
                    ScoredUrlDatum scoreUrl = new ScoredUrlDatum(curTuple, _metaDataFields);
                    if (queue.offer(scoreUrl)) {
                        numUrlsQueued += 1;
                    } else {
                        numUrlsSkipped += 1;
                    }
                }

                // Typically we're using the IP address for the domain, so extract a
                // representative host name from the first item's URL.
                String host = queue.getHost();

                // We're going to spin here until the queue manager decides that we
                // have available space for this next queue.
                // TODO KKr - have timeout here based on target fetch duration. Or we might
                // want to have the offer() method do this automatically for us, hmm but then
                // we'd need it to call process.keepAlive().
                while (!_queueMgr.offer(queue)) {
                    process.keepAlive();
                    Thread.sleep(OFFER_QUEUE_DELAY);
                }

                _flowProcess.increment(FetchCounters.URLS_QUEUED, numUrlsQueued);
                _flowProcess.increment(FetchCounters.URLS_REMAINING, numUrlsQueued);
                _flowProcess.increment(FetchCounters.URLS_SKIPPED, numUrlsSkipped);

                if (numUrlsQueued > 0) {
                    LOGGER.info(String.format("Queued %d URLs from %s (%s)", numUrlsQueued, domain, host));
                }
                
                if (numUrlsSkipped > 0) {
                    LOGGER.info(String.format("Skipped %d URLs from %s (%s)", numUrlsSkipped, domain, host));
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Exception during creating of fetcher queues", t);
            // TODO KKr - don't lose any of the URLs
        }
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall operationCall) {
        try {
            boolean skippedAll = false;
            while (!_fetcherMgr.isDone()) {
                process.keepAlive();
                Thread.sleep(1000L);
                
                long endTime = _fetcherPolicy.getCrawlEndTime();
                if (!skippedAll && (endTime != FetcherPolicy.NO_CRAWL_END_TIME) && (System.currentTimeMillis() >= endTime)) {
                    _queueMgr.skipAll(UrlStatus.SKIPPED_TIME_LIMIT);
                    skippedAll = true;
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
        } catch (InterruptedException e) {
            LOGGER.error("Interruption while waiting for fetcher manager to finish");
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
