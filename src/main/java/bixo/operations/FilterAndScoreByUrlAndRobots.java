package bixo.operations;

import java.util.Iterator;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.cascading.NullContext;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.ScoreGenerator;
import bixo.hadoop.FetchCounters;
import bixo.utils.DiskQueue;
import bixo.utils.GroupingKey;
import bixo.utils.ThreadedExecutor;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Filter out URLs by either domain (not popular enough) or if they're blocked by robots.txt
 *
 */

@SuppressWarnings("serial")
public class FilterAndScoreByUrlAndRobots extends BaseOperation<NullContext> implements Buffer<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(FilterAndScoreByUrlAndRobots.class);
	
    // Some robots.txt files are > 64K, amazingly enough.
    private static final int MAX_ROBOTS_SIZE = 128 * 1024;

    // subdomain.domain.com can direct to domain.com, so if we're simultaneously fetching
    // a bunch of robots from subdomains that redirect, we'll exceed the default limit.
    private static final int MAX_CONNECTIONS_PER_HOST = 20;
    
    // Crank down default values when fetching robots.txt, as this should be super
    // fast to get back.
    private static final int ROBOTS_CONNECTION_TIMEOUT = 10 * 1000;
    private static final int ROBOTS_SOCKET_TIMEOUT = 10 * 1000;
    private static final int ROBOTS_RETRY_COUNT = 2;

    // TODO KKr - set up min response rate, use it with max size to calc max
    // time for valid download, use it for COMMAND_TIMEOUT
    
    // Amount of time we'll wait for pending tasks to finish up. This is roughly equal
    // to the max amount of time it might take to fetch a robots.txt file (excluding
    // download time, which we could add).
    // FUTURE KKr - add in time to do the download.
    private static final long COMMAND_TIMEOUT = (ROBOTS_CONNECTION_TIMEOUT + ROBOTS_SOCKET_TIMEOUT) * ROBOTS_RETRY_COUNT;

    private static final long TERMINATE_TIMEOUT = COMMAND_TIMEOUT;

    private static final int MAX_URLS_IN_MEMORY = 100;

    private ScoreGenerator _scorer;
    private Fields _metadataFields;
	private IHttpFetcher _fetcher;
	
    private transient ThreadedExecutor _executor;
    private transient BixoFlowProcess _flowProcess;

    public FilterAndScoreByUrlAndRobots(UserAgent userAgent, int maxThreads, ScoreGenerator scorer, Fields metadataFields) {
        // We're going to output a ScoredUrlDatum (what FetcherBuffer expects).
        super(ScoredUrlDatum.FIELDS.append(metadataFields));

        _scorer = scorer;
        _metadataFields = metadataFields;
        _fetcher = createFetcher(userAgent, maxThreads);
    }

    public FilterAndScoreByUrlAndRobots(IHttpFetcher fetcher, ScoreGenerator scorer, Fields metadataFields) {
        // We're going to output a ScoredUrlDatum (what FetcherBuffer expects).
        super(ScoredUrlDatum.FIELDS.append(metadataFields));

        _scorer = scorer;
        _metadataFields = metadataFields;
        _fetcher = fetcher;
    }

    public static IHttpFetcher createFetcher(UserAgent userAgent, int maxThreads) {
        // TODO KKr - add static createRobotsFetcher method somewhere that
        // I can use here, and also in SimpleGroupingKeyGenerator
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxContentSize(MAX_ROBOTS_SIZE);
        policy.setMaxConnectionsPerHost(MAX_CONNECTIONS_PER_HOST);
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, policy, userAgent);
        fetcher.setMaxRetryCount(ROBOTS_RETRY_COUNT);
        fetcher.setConnectionTimeout(ROBOTS_CONNECTION_TIMEOUT);
        fetcher.setSocketTimeout(ROBOTS_SOCKET_TIMEOUT);
        
        return fetcher;
    }
    
    @Override
    public void prepare(FlowProcess flowProcess, cascading.operation.OperationCall<NullContext> operationCall) {
        _executor = new ThreadedExecutor(_fetcher.getMaxThreads(), COMMAND_TIMEOUT);
        
        // FUTURE KKr - use Cascading process vs creating our own, once it
        // supports logging in local mode, and a setStatus() call.
        // FUTURE KKr - check for a serialized external reporter in the process,
        // add it if it exists.
        _flowProcess = new BixoFlowProcess((HadoopFlowProcess)flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
    };
    
    @Override
    public void cleanup(FlowProcess flowProcess, cascading.operation.OperationCall<NullContext> operationCall) {
        
        try {
            if (!_executor.terminate(TERMINATE_TIMEOUT)) {
                LOGGER.warn("Had to do a hard shutdown of robots fetching");
            }
        } catch (InterruptedException e) {
            // FUTURE What's the right thing to do here? E.g. do I need to worry about
            // losing URLs still to be processed?
            LOGGER.warn("Interrupted while waiting for termination");
        }
        
        _flowProcess.dumpCounters();
    };
    
	@Override
	public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
        TupleEntry group = bufferCall.getGroup();
        String protocolAndDomain = group.getString(0);

        DiskQueue<GroupedUrlDatum> urls = new DiskQueue<GroupedUrlDatum>(MAX_URLS_IN_MEMORY);
        Iterator<TupleEntry> values = bufferCall.getArgumentsIterator();
        while (values.hasNext()) {
            urls.add(new GroupedUrlDatum(values.next().getTuple(), _metadataFields));
        }
        
        try {
            Runnable doRobots = new ProcessRobotsTask(protocolAndDomain, _scorer, urls, _fetcher, bufferCall.getOutputCollector(), _flowProcess);
            _executor.execute(doRobots);
            _flowProcess.increment(FetchCounters.DOMAINS_QUEUED, 1);
            _flowProcess.increment(FetchCounters.DOMAINS_REMAINING, 1);
        } catch (RejectedExecutionException e) {
            // should never happen.
            LOGGER.error("Robots handling pool rejected our request for " + protocolAndDomain);
            _flowProcess.increment(FetchCounters.DOMAINS_REJECTED, 1);
            _flowProcess.increment(FetchCounters.URLS_REJECTED, urls.size());
            ProcessRobotsTask.emptyQueue(urls, GroupingKey.DEFERRED_GROUPING_KEY, bufferCall.getOutputCollector());
        }
	}

	

}
