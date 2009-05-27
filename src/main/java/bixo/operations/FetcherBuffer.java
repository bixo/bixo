package bixo.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.config.FetcherPolicy;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.FetcherCounters;
import bixo.fetcher.FetcherManager;
import bixo.fetcher.FetcherQueue;
import bixo.fetcher.FetcherQueueMgr;
import bixo.fetcher.http.IHttpFetcher;
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
    
    private FetcherManager _fetcherMgr;
    private FetcherQueueMgr _queueMgr;
    private Thread _fetcherThread;
    private BixoFlowProcess _flowProcess;
    private IHttpFetcher _fetcher;

    private final Fields _metaDataFields;

    public FetcherBuffer(Fields outFields, Fields metaDataFields, IHttpFetcher fetcher) {
        super(outFields.append(metaDataFields));

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
            defaultPolicy = new FetcherPolicy();
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
            // <key> is the PLD grouper, while each entry from <values> is a
            // FetchQueueEntry.
            String domain = group.getString(0);

            // TODO KKr - if domain isn't already an IP address, we want to
            // covert URLs to IP addresses and segment that way, as otherwise
            // keep-alive doesn't buy us much if (as on large sites) xxx.domain
            // can go to different servers. Which means breaking it up here
            // into sorted lists, and creating a queue with the list of items
            // to be fetched (moving list logic elsewhere??)
            
            // Really what we want is to create N queues for N unique combinations
            // of IP address and robots.txt. Which means having a mapping from
            // hostname (full) to IP/robots, and another one from IP/robots to queues.
            // So you get a hostname, and if it doesn't exist in the first table then
            // you map it to IP/robots. If IP/robots doesn't exist in the second table,
            // you create a new queue. Then you add the URL to the right queue.
            //
            // This should handle polite crawling (by IP, and robots.txt). Makes me
            // think we might want to handle this as a regular function that takes
            // URL and adds IP/crawl delay as the key (and filter if blocked). Then
            // group by this, and we're done. Would need good DNS (and probably our
            // own cache in front, for each such function).
            
            FetcherQueue queue = _queueMgr.createQueue(domain, buffCall.getOutputCollector());

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
            LOGGER.debug(String.format("Skipping %d URLs from %s", skipped, domain));
            
            _flowProcess.increment(FetcherCounters.DOMAINS_QUEUED, 1);
        } catch (Throwable t) {
            LOGGER.error("Exception during reduce", t);
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

}
