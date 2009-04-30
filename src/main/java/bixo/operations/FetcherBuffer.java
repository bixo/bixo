package bixo.operations;

import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.FetcherCounters;
import bixo.fetcher.FetcherManager;
import bixo.fetcher.FetcherQueue;
import bixo.fetcher.FetcherQueueMgr;
import bixo.fetcher.http.IHttpFetcherFactory;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings( { "serial", "unchecked" })
public class FetcherBuffer extends BaseOperation implements cascading.operation.Buffer {
    private static Logger LOGGER = Logger.getLogger(FetcherBuffer.class);

    private FetcherManager _fetcherMgr;
    private FetcherQueueMgr _queueMgr;
    private Thread _fetcherThread;
    private BixoFlowProcess _flowProcess;
    private IHttpFetcherFactory _fetcherFactory;

    private final Fields _metaDataFields;

    public FetcherBuffer(Fields outFields, Fields metaDataFields, IHttpFetcherFactory factory) {
        super(outFields.append(metaDataFields));

        _metaDataFields = metaDataFields;
        _fetcherFactory = factory;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        super.prepare(flowProcess, operationCall);

        // FUTURE KKr - use Cascading process vs creating our own, once it
        // supports
        // logging in local mode, and a setStatus() call.
        // TODO KKr - check for a serialized external reporter in the process,
        // add
        // it if it exists.
        _flowProcess = new BixoFlowProcess((HadoopFlowProcess) flowProcess);

        _queueMgr = new FetcherQueueMgr();
        _fetcherMgr = new FetcherManager(_queueMgr, _fetcherFactory, _flowProcess);

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
            FetcherPolicy policy = new FetcherPolicy();

            // TODO KKr - base maxURLs on fetcher policy, target end of fetch
            int maxURLs = 10;

            // TODO KKr - if domain isn't already an IP address, we want to
            // covert URLs to IP addresses and segment that way, as otherwise
            // keep-alive doesn't buy us much if (as on large sites) xxx.domain
            // can go to different servers. Which means breaking it up here
            // into sorted lists, and creating a queue with the list of items
            // to be fetched (moving list logic elsewhere??)
            FetcherQueue queue = new FetcherQueue(domain, policy, maxURLs, _flowProcess, buffCall.getOutputCollector());

            int skipped = 0;
            while (values.hasNext()) {
                Tuple curTuple = values.next().getTuple();
                ScoredUrlDatum scoreUrl = new ScoredUrlDatum(curTuple, _metaDataFields);

                if (!queue.offer(scoreUrl)) {
                    skipped += 1;
                }
            }

            _flowProcess.increment(FetcherCounters.URLS_QUEUED, queue.size());
            _flowProcess.increment(FetcherCounters.URLS_SKIPPED, skipped);

            // We're going to spin here until the queue manager decides that we
            // have available space for this next queue.
            // TODO KKr - have timeout here based on target fetch duration.
            while (!_queueMgr.offer(queue)) {
                process.keepAlive();
            }

            _flowProcess.increment(FetcherCounters.ADDED_DOMAIN_QUEUE, 1);
        } catch (Throwable t) {
            LOGGER.error("Exception during reduce", t);
        }

    }

    @Override
    public void cleanup(FlowProcess process, OperationCall operationCall) {
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
    }

}
