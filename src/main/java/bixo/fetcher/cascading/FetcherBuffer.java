package bixo.fetcher.cascading;

import java.util.Iterator;

import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import bixo.fetcher.FetcherManager;
import bixo.fetcher.FetcherQueue;
import bixo.fetcher.FetcherQueueMgr;
import bixo.fetcher.IHttpFetcherFactory;
import bixo.fetcher.beans.FetchItem;
import bixo.fetcher.beans.FetcherPolicy;
import bixo.tuple.UrlWithScoreTuple;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class FetcherBuffer extends BaseOperation<String> implements cascading.operation.Buffer<String> {

    private static Logger LOG = Logger.getLogger(FetcherBuffer.class);
    private FetcherManager _fetcherMgr;
    private FetcherQueueMgr _queueMgr;
    private Thread _fetcherThread;

    private IHttpFetcherFactory _fetcherFactory;

    public FetcherBuffer(IHttpFetcherFactory factory) {
        _fetcherFactory = factory;
    }

    // private FetchCollector _collector;

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<String> operationCall) {
        super.prepare(flowProcess, operationCall);
        // JobConf jobConf = ((HadoopFlowProcess) flowProcess).getJobConf();
        // _collector = new FetchCollector(jobConf);

        _queueMgr = new FetcherQueueMgr();
        // TODO KKr- configure max threads in conf?

        _fetcherMgr = new FetcherManager(_queueMgr, _fetcherFactory);

        _fetcherThread = new Thread(_fetcherMgr);
        _fetcherThread.setName("Fetcher manager");
        _fetcherThread.start();

    }

    @Override
    public void operate(FlowProcess process, BufferCall<String> buffCall) {
        Iterator<TupleEntry> values = buffCall.getArgumentsIterator();
        TupleEntry group = buffCall.getGroup();
        Reporter reporter = ((HadoopFlowProcess) process).getReporter();

        try {
            // <key> is the PLD grouper, while each entry from <values> is a
            // FetchItem.
            String domain = group.getString(0);
            FetcherPolicy policy = new FetcherPolicy();

            // TODO KKr - base maxURLs on fetcher policy, target end of fetch
            int maxURLs = 10;

            FetcherQueue queue = new FetcherQueue(domain, policy, maxURLs);

            while (values.hasNext()) {
                FetchItem item = new FetchItem(new UrlWithScoreTuple(values.next().getTuple()), buffCall.getOutputCollector());
                queue.offer(item);
            }

            while (!_queueMgr.offer(queue)) {
                reporter.progress();
            }
        } catch (Throwable t) {
            LOG.error("Exception during reduce: " + t.getMessage(), t);
        }

    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<String> operationCall) {

        Reporter reporter = ((HadoopFlowProcess) flowProcess).getReporter();
        while (!_fetcherMgr.isDone()) {
            if (reporter != null) {
                reporter.progress();
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
            }

        }

        _fetcherThread.interrupt();
        // try {
        // _collector.close();
        // } catch (IOException e) {
        // throw new RuntimeException("Unable to close collector", e);
        // }
    }

}
