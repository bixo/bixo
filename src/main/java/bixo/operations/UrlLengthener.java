package bixo.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.RedirectMode;
import bixo.config.UserAgent;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.hadoop.FetchCounters;
import bixo.utils.ThreadedExecutor;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class UrlLengthener extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(UrlLengthener.class);
    
    public static final String URL_FN = "url";
    
    // Seems like things work better when we fetch the content versus aborting the connection
    private static final int MAX_CONTENT_SIZE = 2 * 1024;
    
    // Lots of sites fail when they get a HEAD request, so avoid many of those by only
    // processing a single redirect (typically from the link shortening service)
    private static final int MAX_REDIRECTS = 1;
    
    private static final int REDIRECT_CONNECTION_TIMEOUT = 20 * 1000;
    private static final int REDIRECT_SOCKET_TIMEOUT = 10 * 1000;
    private static final int REDIRECT_RETRY_COUNT = 1;

    private static final long COMMAND_TIMEOUT = (REDIRECT_CONNECTION_TIMEOUT + REDIRECT_SOCKET_TIMEOUT) * REDIRECT_RETRY_COUNT;
    private static final long TERMINATE_TIMEOUT = COMMAND_TIMEOUT * 2;

    private static final Pattern HOSTNAME_PATTERN = Pattern.compile("^http://([^/:?]{3,})");
    
    private BaseFetcher _fetcher;
    private int _maxThreads;
    private Set<String> _urlShorteners;

    private transient LoggingFlowProcess _flowProcess;
    private transient TupleEntryCollector _collector;
    private transient ThreadedExecutor _executor;

    public static BaseFetcher makeFetcher(int maxThreads, UserAgent userAgent) {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setRedirectMode(RedirectMode.FOLLOW_NONE);
        policy.setMaxRedirects(MAX_REDIRECTS);
        policy.setMaxConnectionsPerHost(maxThreads);

        SimpleHttpFetcher result = new SimpleHttpFetcher(maxThreads, policy, userAgent);
        result.setDefaultMaxContentSize(MAX_CONTENT_SIZE);
        
        // We don't want any encoding (compression) of the data.
        result.setAcceptEncoding("");
        return result;
    }
    
    public UrlLengthener(BaseFetcher fetcher, int maxThreads) throws IOException {
        super(new Fields(URL_FN));
        
        _fetcher = fetcher;
        _maxThreads = maxThreads;
        
        _urlShorteners = loadUrlShorteners();
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        
        _flowProcess = new LoggingFlowProcess((HadoopFlowProcess) flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());

        _executor = new ThreadedExecutor(_maxThreads, COMMAND_TIMEOUT);
    }
    
    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        try {
            if (!_executor.terminate(TERMINATE_TIMEOUT)) {
                LOGGER.warn("Had to do a hard shutdown of robots fetching");
            }
        } catch (InterruptedException e) {
            // FUTURE What's the right thing to do here? E.g. do I need to worry about
            // losing URLs still to be processed?
            LOGGER.warn("Interrupted while waiting for termination");
            Thread.currentThread().interrupt();
        }
        
        _flowProcess.dumpCounters();
        super.cleanup(flowProcess, operationCall);
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        _collector = functionCall.getOutputCollector();

        String url = functionCall.getArguments().getTuple().getString(0);
        
        // Figure out if this is a URL from a shortener service.
        // If so, then we want to try to lengthen it.
        // If not, see if it looks like shortened URL, and try anyway.
        
        Matcher m = HOSTNAME_PATTERN.matcher(url);
        if (!m.find()) {
            emitTuple(url);
            return;
        }

        String hostname = m.group(1);
        if (!_urlShorteners.contains(hostname)) {
            // FUTURE - see if this looks like a shortened URL
            emitTuple(url);
            return;
        }
        
        try {
            ResolveRedirectsTask task = new ResolveRedirectsTask(url, _fetcher, _collector);
            _executor.execute(task);
        } catch (RejectedExecutionException e) {
            // should never happen.
            LOGGER.error("Redirection handling pool rejected our request for " + url);
            _flowProcess.increment(FetchCounters.URLS_REJECTED, 1);
            
            emitTuple(url);
        } catch (Throwable t) {
            LOGGER.error("Caught an unexpected throwable - redirection code rejected our request for " + url, t);
            _flowProcess.increment(FetchCounters.URLS_REJECTED, 1);
            
            emitTuple(url);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static Set<String> loadUrlShorteners() throws IOException {
        Set<String> result = new HashSet<String>();
        List<String> lines = IOUtils.readLines(UrlLengthener.class.getResourceAsStream("/url-shorteners.txt"), "UTF-8");
        for (String line : lines) {
            line = line.trim();
            if ((line.length() == 0) || (line.startsWith("#"))) {
                continue;
            }
            
            int commentIndex = line.indexOf('#');
            if (commentIndex != -1) {
                line = line.substring(0, commentIndex).trim();
            }
            
            result.add(line);
        }
        
        return result;
    }
    

    private void emitTuple(String url) {
        synchronized(_collector) {
            _collector.add(new Tuple(url));
        }
    }
    
}
