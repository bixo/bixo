package bixo.pipes;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import bixo.cascading.NullSinkTap;
import bixo.datum.FetchedDatum;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.StatusDatum;
import bixo.exceptions.BaseFetchException;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.IScoreGenerator;
import bixo.fetcher.util.SimpleGroupingKeyGenerator;
import bixo.fetcher.util.SimpleScoreGenerator;
import bixo.operations.FetcherBuffer;
import bixo.operations.GroupFunction;
import bixo.operations.ScoreFunction;
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

@SuppressWarnings("serial")
public class FetchPipe extends SubAssembly {
    // Pipe that outputs FetchedDatum tuples, for URLs that were fetched.
    public static final String FETCHED_PIPE_NAME = "fetched";
    
    // Pipe that outputs StatusDatum tuples, for all URLs being processed.
    public static final String STATUS_PIPE_NAME = "status";
    
    @SuppressWarnings({ "unchecked" })
    private static class FilterErrorsFunction extends BaseOperation implements Function {
        private int _fieldPos;
        private int[] _fieldsToCopy;
        
        // Only output FetchedDatum tuples for input where we were able to fetch the URL.
        public FilterErrorsFunction(Fields resultFields) {
            super(resultFields.size() + 1, resultFields);
            
            // Location of extra field added during fetch, that contains fetch error
            _fieldPos = resultFields.size();
            
            // Create array used to extract the fields we need that correspond to
            // the FetchedDatum w/o the exception tacked on the end.
            _fieldsToCopy = new int[resultFields.size()];
            for (int i = 0; i < _fieldsToCopy.length; i++) {
                _fieldsToCopy[i] = i;
            }
        }

        @Override
        public void operate(FlowProcess process, FunctionCall funcCall) {
            Tuple t = funcCall.getArguments().getTuple();
            
            // If we get a "no error" string instead of an exception, it's a good fetch.
            if (t.get(_fieldPos) instanceof String) {
                funcCall.getOutputCollector().add(t.get(_fieldsToCopy));
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    private static class MakeStatusFunction extends BaseOperation implements Function {
        private int _fieldPos;
        private Fields _metaDataFields;
        
        // Output an appropriate StatusDatum based on whether we were able to fetch
        // the URL or not.
        public MakeStatusFunction(Fields inputFields, Fields metaDataFields) {
            super(StatusDatum.FIELDS.append(metaDataFields));
            
            // Location of extra field added during fetch, that contains fetch error
            _fieldPos = inputFields.size();
            
            _metaDataFields = metaDataFields;
        }

        @Override
        public void operate(FlowProcess process, FunctionCall funcCall) {
            Tuple t = funcCall.getArguments().getTuple();
            FetchedDatum fd = new FetchedDatum(t, _metaDataFields);
            Comparable result = t.get(_fieldPos);
            StatusDatum status;
            
            if (result instanceof String) {
                status = new StatusDatum(fd.getBaseUrl(), fd.getHeaders(), fd.getMetaDataMap());
            } else {
                status = new StatusDatum(fd.getBaseUrl(), (BaseFetchException)result, fd.getMetaDataMap());
            }
            
            funcCall.getOutputCollector().add(status.toTuple());
        }
    }

    /**
     * Create FetchPipe with default SimpleXXX classes and default parameters.
     * 
     * @param urlProvider Source for URLs - must output UrlDatum tuples
     * @param userAgent name to use during fetching
     */
    public FetchPipe(Pipe urlProvider, String userAgent) {
        this(urlProvider, new SimpleGroupingKeyGenerator(userAgent), new SimpleScoreGenerator(), new SimpleHttpFetcher(userAgent), new Fields());
    }
    
    public FetchPipe(Pipe urlProvider, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcher fetcher) {
        this(urlProvider, keyGenerator, scoreGenerator, fetcher, new Fields());
    }

    public FetchPipe(Pipe urlProvider, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcher fetcher, Fields metaDataFields) {
        Pipe fetch = new Pipe("fetch_pipe", urlProvider);

        Fields groupedFields = GroupedUrlDatum.FIELDS.append(metaDataFields);
        fetch = new Each(fetch, new GroupFunction(metaDataFields, keyGenerator), groupedFields);

        Fields scoredFields = ScoredUrlDatum.FIELDS.append(metaDataFields);
        fetch = new Each(fetch, new ScoreFunction(scoreGenerator, metaDataFields), scoredFields);

        fetch = new GroupBy(fetch, new Fields(GroupedUrlDatum.GROUP_KEY_FIELD));
        fetch = new Every(fetch, new FetcherBuffer(metaDataFields, fetcher), Fields.RESULTS);

        Fields fetchedFields = FetchedDatum.FIELDS.append(metaDataFields);
        Pipe fetched = new Pipe(FETCHED_PIPE_NAME, new Each(fetch, new FilterErrorsFunction(fetchedFields)));
        Pipe status = new Pipe(STATUS_PIPE_NAME, new Each(fetch, new MakeStatusFunction(fetchedFields, metaDataFields)));
        
        setTails(fetched, status);
    }
    
    public Pipe getTailPipe(String pipeName) {
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
        result.put(FETCHED_PIPE_NAME, fetchedSink);
        
        return result;
    }
}
