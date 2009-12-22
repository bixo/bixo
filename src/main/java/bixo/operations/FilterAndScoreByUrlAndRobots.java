package bixo.operations;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.ScoreGenerator;
import bixo.operations.ProcessRobotsTask.DomainInfo;
import bixo.utils.DiskQueue;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Filter out URLs by either domain (not popular enough) or if they're blocked by robots.txt
 *
 */

@SuppressWarnings("serial")
public class FilterAndScoreByUrlAndRobots extends BaseOperation<NullContext> implements Buffer<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(FilterAndScoreByUrlAndRobots.class);
	
    // Some robots.txt files are > 64K
    private static final int MAX_ROBOTS_SIZE = 128 * 1024;

    private static final int FETCH_THREAD_COUNT_CORE = 10;
    private static final int FETCH_IDLE_TIMEOUT = 1;

    // Crank down default values when fetching robots.txt, as this should be super
    // fast to get back.
    private static final int ROBOTS_CONNECTION_TIMEOUT = 10 * 1000;
    private static final int ROBOTS_SOCKET_TIMEOUT = 10 * 1000;
    private static final int ROBOTS_RETRY_COUNT = 2;

    // Amount of time we'll wait for pending tasks to finish up.
    private static final long TERMINATION_TIMEOUT_SECONDS = 120;

    // How long we wait (in ms) between each check for an open thread to use.
    private static final long NO_CAPACITY_SLEEP_INTERVAL = 100L;

    private static final int MAX_URLS_IN_MEMORY = 100;

    private ScoreGenerator _scorer;
    private Fields _metadataFields;
	private IHttpFetcher _fetcher;
	
    private transient ThreadPoolExecutor _pool;

    public FilterAndScoreByUrlAndRobots(UserAgent userAgent, int maxThreads, ScoreGenerator scorer, Fields metadataFields) {
        // We're going to output a ScoredUrlDatum (what FetcherBuffer expects).
        super(ScoredUrlDatum.FIELDS.append(metadataFields));

        // TODO KKr - add static createRobotsFetcher method somewhere that
        // I can use here, and also in SimpleGroupingKeyGenerator
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxContentSize(MAX_ROBOTS_SIZE);
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, policy, userAgent);
        fetcher.setMaxRetryCount(ROBOTS_RETRY_COUNT);
        fetcher.setConnectionTimeout(ROBOTS_CONNECTION_TIMEOUT);
        fetcher.setSocketTimeout(ROBOTS_SOCKET_TIMEOUT);
        
        _fetcher = fetcher;
        _scorer = scorer;
        _metadataFields = metadataFields;
    }

    public FilterAndScoreByUrlAndRobots(IHttpFetcher fetcher, ScoreGenerator scorer, Fields metadataFields) {
        // We're going to output a ScoredUrlDatum (what FetcherBuffer expects).
        super(ScoredUrlDatum.FIELDS.append(metadataFields));

        _fetcher = fetcher;
        _scorer = scorer;
        _metadataFields = metadataFields;
    }

    @Override
    public void prepare(FlowProcess flowProcess, cascading.operation.OperationCall<NullContext> operationCall) {
        int maxThreads = _fetcher.getMaxThreads();
        
        SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>(true);
        _pool = new ThreadPoolExecutor(Math.min(FETCH_THREAD_COUNT_CORE, maxThreads), maxThreads,
                        FETCH_IDLE_TIMEOUT, TimeUnit.SECONDS, queue);
        _pool.allowCoreThreadTimeOut(true);
    };
    
    @Override
    public void cleanup(FlowProcess flowProcess, cascading.operation.OperationCall<NullContext> operationCall) {
        _pool.shutdown();
        
        try {
            long startTime = System.currentTimeMillis();
            if (!_pool.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                // Since should only have as many entries in the queue as we have threads,
                // we should never have to wait very long for robots.txt files to be fetched.
                LOGGER.error("Had to force shutdown the robot thread pool");
                _pool.shutdownNow();
            } else {
                LOGGER.trace("Shutdown took " + (System.currentTimeMillis() - startTime) + "ms");
            }
        } catch (InterruptedException e) {
            // FUTURE What's the right thing to do here? E.g. do I need to worry about
            // losing URLs still to be processed?
            LOGGER.warn("Interrupted while waiting for termination");
        }
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
        
        DomainInfo domainInfo;
        try {
            domainInfo = new DomainInfo(protocolAndDomain);
        } catch (UnknownHostException e) {
            LOGGER.trace("Unknown host: " + protocolAndDomain);
            emptyQueue(urls, GroupingKey.UNKNOWN_HOST_GROUPING_KEY, bufferCall.getOutputCollector());
            return;
        } catch (Exception e) {
            LOGGER.warn("Exception processing " + protocolAndDomain, e);
            emptyQueue(urls, GroupingKey.INVALID_URL_GROUPING_KEY, bufferCall.getOutputCollector());
            return;
        }

        Runnable doRobots = new ProcessRobotsTask(domainInfo, _scorer, urls, _fetcher, bufferCall.getOutputCollector());
        while (_pool.getActiveCount() == _pool.getMaximumPoolSize()) {
            // Spin until we have a slot.
            try {
                Thread.sleep(NO_CAPACITY_SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for available thread");
                emptyQueue(urls, GroupingKey.DEFERRED_GROUPING_KEY, bufferCall.getOutputCollector());
                return;
            }
        }
        
        try {
            _pool.execute(doRobots);
        } catch (RejectedExecutionException e) {
            // should never happen.
            LOGGER.error("Robots handling pool rejected our request: " + _pool.getActiveCount() + " active threads");
            emptyQueue(urls, GroupingKey.DEFERRED_GROUPING_KEY, bufferCall.getOutputCollector());
        }
	}

    /**
     * Clear out the queue by outputting all entries with <groupingKey>.
     * 
     * We do this to empty the queue when there's some kind of error.
     * 
     * @param urls Queue of URLs to empty out
     * @param groupingKey grouping key to use for all entries.
     * @param outputCollector
     */
    private void emptyQueue(DiskQueue<GroupedUrlDatum> urls, String groupingKey, TupleEntryCollector outputCollector) {
        GroupedUrlDatum datum;
        while ((datum = urls.poll()) != null) {
            ScoredUrlDatum scoreUrl = new ScoredUrlDatum(datum.getUrl(), 0, 0, UrlStatus.UNFETCHED, groupingKey, 1.0, datum.getMetaDataMap());
            outputCollector.add(scoreUrl.toTuple());
        }
    }
	

}
