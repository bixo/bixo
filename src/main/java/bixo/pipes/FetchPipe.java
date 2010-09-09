package bixo.pipes;

import java.net.MalformedURLException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import bixo.cascading.ISplitter;
import bixo.cascading.NullContext;
import bixo.cascading.NullSinkTap;
import bixo.cascading.SplitterAssembly;
import bixo.datum.FetchedDatum;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.FetchSetDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.FetchException;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.operations.FetchBuffer;
import bixo.operations.FilterAndScoreByUrlAndRobots;
import bixo.operations.GroupFunction;
import bixo.operations.MakeFetchSetsBuffer;
import bixo.robots.RobotRulesParser;
import bixo.robots.SimpleRobotRulesParser;
import bixo.utils.GroupingKey;
import bixo.utils.UrlUtils;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class FetchPipe extends SubAssembly {
        
    // Pipe that outputs FetchedDatum tuples, for URLs that were fetched.
    public static final String CONTENT_PIPE_NAME = "FetchPipe-content";
    
    // Pipe that outputs StatusDatum tuples, for all URLs being processed.
    public static final String STATUS_PIPE_NAME = "FetchPipe-status";
    
    /**
     * Generate key using protocol+host+port, which is what we need in order
     * to safely fetch robots.txt files.
     *
     */
    private static class GroupByDomain implements IGroupingKeyGenerator {
        
        @Override
        public String getGroupingKey(UrlDatum urlDatum) {
            String urlAsString = urlDatum.getUrl();
            
            try {
                return UrlUtils.makeProtocolAndDomain(urlAsString);
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid URL: " + urlAsString);
            }
        }
    }

    private static class SplitIntoSpecialAndRegularKeys implements ISplitter {

        @Override
        public String getLHSName() {
            return "special grouping key";
        }

        @Override
        public boolean isLHS(TupleEntry tuple) {
            ScoredUrlDatum datum = new ScoredUrlDatum(tuple);
            return GroupingKey.isSpecialKey(datum.getGroupKey());
        }
    }
    
    @SuppressWarnings({ "unchecked" })
    private static class FilterErrorsFunction extends BaseOperation implements Function {
        private int _fieldPos;
        private int[] _fieldsToCopy;
        
        // Only output FetchedDatum tuples for input where we were able to fetch the URL.
        public FilterErrorsFunction() {
            super(FetchedDatum.FIELDS.size() + 1, FetchedDatum.FIELDS);
            int baseFieldCount = FetchedDatum.FIELDS.size();
            
            // Location of extra field added during fetch, that contains fetch error
            _fieldPos = baseFieldCount;
            
            // Create array used to extract the fields we need that correspond to
            // the FetchedDatum w/o the exception tacked on the end.
            _fieldsToCopy = new int[baseFieldCount];
            for (int i = 0; i < _fieldsToCopy.length; i++) {
                _fieldsToCopy[i] = i;
            }
        }

        @Override
        public void operate(FlowProcess process, FunctionCall funcCall) {
            Tuple t = funcCall.getArguments().getTuple();
            
            // Get the status to decide if it's a good fetch
            Comparable status = t.get(_fieldPos);
            if ((status instanceof String) && (UrlStatus.valueOf((String)status) == UrlStatus.FETCHED)) {
                funcCall.getOutputCollector().add(t.get(_fieldsToCopy));
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    private static class MakeStatusFunction extends BaseOperation implements Function {
        private int _fieldPos;
        
        // Output an appropriate StatusDatum based on whether we were able to fetch
        // the URL or not.
        public MakeStatusFunction() {
            super(StatusDatum.FIELDS);
            
            // Location of extra field added during fetch, that contains fetch status
            _fieldPos = FetchedDatum.FIELDS.size();
        }

        @Override
        public void operate(FlowProcess process, FunctionCall funcCall) {
            TupleEntry entry = funcCall.getArguments();
            FetchedDatum fd = new FetchedDatum(entry);
            
            // Get the fetch status that we hang on the end of the tuple,
            // after all of the FetchedDatum fields.
            Comparable result = entry.get(_fieldPos);
            StatusDatum status;
            
            if (result instanceof String) {
                UrlStatus urlStatus = UrlStatus.valueOf((String)result);
                if (urlStatus == UrlStatus.FETCHED) {
                    status = new StatusDatum(fd.getBaseUrl(), fd.getHeaders(), fd.getHostAddress());
                } else {
                    status = new StatusDatum(fd.getBaseUrl(), urlStatus);
                }
            } else if (result instanceof FetchException) {
                status = new StatusDatum(fd.getBaseUrl(), (FetchException)result);
            } else {
                throw new RuntimeException("Unknown type for fetch status field: " + result.getClass());
            }
            
            status.setPayload(fd);
            funcCall.getOutputCollector().add(status.getTuple());
        }
    }

    private static class MakeSkippedStatus extends BaseOperation<NullContext> implements Function<NullContext> {
        
        // Output an appropriate StatusDatum based on the grouping key (which must be special)
        public MakeSkippedStatus() {
            super(StatusDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            ScoredUrlDatum sd = new ScoredUrlDatum(funcCall.getArguments());
            
            String key = sd.getGroupKey();
            if (!GroupingKey.isSpecialKey(key)) {
                throw new RuntimeException("Can't make skipped status for regular grouping key: " + key);
            }
            
            StatusDatum status = new StatusDatum(sd.getUrl(), GroupingKey.makeUrlStatusFromKey(key));
            status.setPayload(sd);
            
            funcCall.getOutputCollector().add(status.getTuple());
        }
    }

    /**
     * Generate an assembly that will fetch all of the UrlDatum tuples coming out of urlProvider.
     * 
     * We assume that these UrlDatums have been validated, and thus we'll only have valid URLs.
     * 
     * @param urlProvider
     * @param scorer
     * @param fetcher
     * @param metaDataFields
     */
    
    public FetchPipe(Pipe urlProvider, ScoreGenerator scorer, IHttpFetcher fetcher) {
        this(urlProvider, scorer, fetcher, null, null, 1);
    }

    public FetchPipe(Pipe urlProvider, ScoreGenerator scorer, IHttpFetcher fetcher, int numReducers) {
        this(urlProvider, scorer, fetcher, null, null, numReducers);
    }
    
    public FetchPipe(Pipe urlProvider, ScoreGenerator scorer, IHttpFetcher fetcher, IHttpFetcher robotsFetcher, RobotRulesParser parser,
                    int numReducers) {
        
        Pipe robotsPipe = new Each(urlProvider, new GroupFunction(new GroupByDomain()));
        robotsPipe = new GroupBy("Grouping URLs by IP/delay", robotsPipe, GroupedUrlDatum.getGroupingField());
        
        if (parser == null) {
            parser = new SimpleRobotRulesParser();
        }
        
        FilterAndScoreByUrlAndRobots filter;
        if (robotsFetcher != null) {
            filter = new FilterAndScoreByUrlAndRobots(robotsFetcher, parser, scorer);
        } else {
            filter = new FilterAndScoreByUrlAndRobots(fetcher.getUserAgent(), fetcher.getMaxThreads(), parser, scorer);
        }
        
        robotsPipe = new Every(robotsPipe, filter, Fields.RESULTS);
        
        // Split into records for URLs that are special (not fetchable) and regular
        SplitterAssembly splitter = new SplitterAssembly(robotsPipe, new SplitIntoSpecialAndRegularKeys());
        
        // Now generate sets of URLs to fetch. We'll wind up with all URLs for the same server & the same crawl delay,
        // ordered by score, getting passed per list to the PreFetchBuffer. This will generate PreFetchDatums that contain a key
        // based on the hash of the IP address (with a range of values == number of reducers), plus a list of URLs and a target
        // crawl time.
        Pipe prefetchPipe = new GroupBy("Distributing URL sets", splitter.getRHSPipe(), GroupedUrlDatum.getGroupingField(), ScoredUrlDatum.getSortingField(), true);
        prefetchPipe = new Every(prefetchPipe, new MakeFetchSetsBuffer(fetcher.getFetcherPolicy(), numReducers), Fields.RESULTS);
        
        Pipe fetchPipe = new GroupBy("Fetching URL sets", prefetchPipe, FetchSetDatum.getGroupingField(), FetchSetDatum.getSortingField());
        fetchPipe = new Every(fetchPipe, new FetchBuffer(fetcher), Fields.RESULTS);

        Pipe fetchedContent = new Pipe(CONTENT_PIPE_NAME, new Each(fetchPipe, new FilterErrorsFunction()));
        
        Pipe fetchedStatus = new Pipe("fetched status", new Each(fetchPipe, new MakeStatusFunction()));
        
        // We need to merge URLs from the LHS of the splitter (never fetched) so that our status pipe
        // gets status for every URL we put into this sub-assembly.
        Pipe skippedStatus = new Pipe("skipped status", new Each(splitter.getLHSPipe(), new MakeSkippedStatus()));
        
        // TODO KKr You're already setting the group name here (so that the
        // tail pipe gets the same name), so I wasn't able to pass in a
        // group name here for BaseTool.nameFlowSteps to use for the job name.
        Pipe joinedStatus = new GroupBy(STATUS_PIPE_NAME, Pipe.pipes(skippedStatus, fetchedStatus), StatusDatum.getGroupingField());

        setTails(fetchedContent, joinedStatus);
    }

    public Pipe getContentTailPipe() {
        return getTailPipe(CONTENT_PIPE_NAME);
    }
    
    public Pipe getStatusTailPipe() {
        return getTailPipe(STATUS_PIPE_NAME);
    }
    
    private Pipe getTailPipe(String pipeName) {
        String[] pipeNames = getTailNames();
        for (int i = 0; i < pipeNames.length; i++) {
            if (pipeName.equals(pipeNames[i])) {
                return getTails()[i];
            }
        }
        
        throw new InvalidParameterException("Invalid pipe name: " + pipeName);
    }

    public static Map<String, Tap> makeSinkMap(Tap statusSink, Tap fetchedSink) {
        HashMap<String, Tap> result = new HashMap<String, Tap>(2);
        
        if (statusSink == null) {
            statusSink = new NullSinkTap(StatusDatum.FIELDS);
        }
        
        if (fetchedSink == null) {
            fetchedSink = new NullSinkTap(FetchedDatum.FIELDS);
        }
        
        result.put(STATUS_PIPE_NAME, statusSink);
        result.put(CONTENT_PIPE_NAME, fetchedSink);
        
        return result;
    }
}
