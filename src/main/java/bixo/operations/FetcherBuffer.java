package bixo.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.config.FetcherPolicy;
import bixo.datum.BaseDatum;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.FetcherCounters;
import bixo.fetcher.FetcherManager;
import bixo.fetcher.FetcherQueue;
import bixo.fetcher.FetcherQueueMgr;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;

@SuppressWarnings( { "serial", "unchecked" })
public class FetcherBuffer extends BaseOperation implements cascading.operation.Buffer {
    private static Logger LOGGER = Logger.getLogger(FetcherBuffer.class);

    public static final String DEFAULT_FETCHER_POLICY_KEY = "bixo.fetcher.default-policy";
    
    private static final Fields FETCH_EXCEPTION_FIELD = new Fields(BaseDatum.fieldName(FetcherBuffer.class, "fetch-exception"));
    
    private FetcherManager _fetcherMgr;
    private FetcherQueueMgr _queueMgr;
    private Thread _fetcherThread;
    private BixoFlowProcess _flowProcess;
    private IHttpFetcher _fetcher;

    private final Fields _metaDataFields;

    public FetcherBuffer(Fields metaDataFields, IHttpFetcher fetcher) {
        // We're going to output a tuple that contains a FetchedDatum, plus meta-data,
        // plus a BaseFetchException object.
        super(FetchedDatum.FIELDS.append(metaDataFields).append(FETCH_EXCEPTION_FIELD));

        _metaDataFields = metaDataFields;
        _fetcher = fetcher;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        super.prepare(flowProcess, operationCall);

        // FUTURE KKr - use Cascading process vs creating our own, once it
        // supports logging in local mode, and a setStatus() call.
        // TODO KKr - check for a serialized external reporter in the process,
        // add it if it exists.
        _flowProcess = new BixoFlowProcess((HadoopFlowProcess) flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
        
        // TODO KKr - remove this support (and fix up searchcrawl's use of it) since you pass
        // in the default fetch policy via <fetcher>.getFetchPolicy() in the constructor. Doh.
        FetcherPolicy defaultPolicy;
        String policyObj = (String)flowProcess.getProperty(DEFAULT_FETCHER_POLICY_KEY);
        if (policyObj != null) {
            try {
                defaultPolicy = (FetcherPolicy)Util.deserializeBase64(policyObj);
                LOGGER.trace("Using serialized fetcher policy: " + defaultPolicy);
            } catch (IOException e) {
                LOGGER.error("Unexpected exception while deserializing the default fetcher policy", e);
                throw new RuntimeException("IOException while deserializing default fetcher policy", e);
            }
        } else {
            defaultPolicy = _fetcher.getFetcherPolicy();
        }
        
        _queueMgr = new FetcherQueueMgr(_flowProcess, defaultPolicy);
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
            // as defined via static values in IGroupingKeyGenerator, or it will be
            // <key>-<crawl delay in ms>
            String key = group.getString(0);

            // TODO KKr - output zombie FetchedUrlDatum w/new BaseFetchException communicating this
            // to the FetchPipe, to split off these entries for URL DB updating.
            if (key.equals(IGroupingKeyGenerator.BLOCKED_GROUPING_KEY)) {
                LOGGER.debug(String.format("Blocked %d URLs", emptyBuffer(values)));

                // URL was blocked by robots.txt
            } else if (key.equals(IGroupingKeyGenerator.UNKNOWN_HOST_GROUPING_KEY)) {
                LOGGER.debug(String.format("Host not found for %d URLs", emptyBuffer(values)));

                // Couldn't resolve hostname to IP address.
            } else if (key.equals(IGroupingKeyGenerator.DEFERRED_GROUPING_KEY)) {
                LOGGER.debug(String.format("Robots.txt problems deferred processing of %d URLs", emptyBuffer(values)));

                // Problem getting/processing robots.txt
            } else {
                String domain = GroupingKey.getDomainFromKey(key);
                long crawlDelay = GroupingKey.getCrawlDelayFromKey(key);
                FetcherQueue queue = _queueMgr.createQueue(domain, buffCall.getOutputCollector(), crawlDelay);

                int skipped = 0;
                int queued = 0;
                while (values.hasNext()) {
                    Tuple curTuple = values.next().getTuple();
                    ScoredUrlDatum scoreUrl = new ScoredUrlDatum(curTuple, _metaDataFields);

                    if (queue.offer(scoreUrl)) {
                        queued += 1;
                    } else {
                        skipped += 1;
                    }
                }

                _flowProcess.increment(FetcherCounters.URLS_QUEUED, queued);
                _flowProcess.increment(FetcherCounters.URLS_SKIPPED, skipped);

                // We're going to spin here until the queue manager decides that we
                // have available space for this next queue.
                // TODO KKr - have timeout here based on target fetch duration.
                while (!_queueMgr.offer(queue)) {
                    process.keepAlive();
                }

                LOGGER.info(String.format("Queued %d URLs from %s", queued, domain));
                LOGGER.debug(String.format("Skipped %d URLs from %s", skipped, domain));

                _flowProcess.increment(FetcherCounters.DOMAINS_QUEUED, 1);
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

    private int emptyBuffer(Iterator<TupleEntry> values) {
        int result = 0;
        while (values.hasNext()) {
            values.next();
            result += 1;
        }
        
        return result;
    }
}
