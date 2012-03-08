/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlMapper;

import bixo.config.FetcherPolicy;
import bixo.config.ParserPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.parser.SimpleLinkExtractor;
import bixo.parser.SimpleParser;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParsePipe;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.IoUtils;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Operation;
import cascading.operation.OperationCall;
import cascading.operation.filter.Limit;
import cascading.operation.filter.Limit.Context;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.OuterJoin;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.BaseSplitter;
import com.bixolabs.cascading.HadoopUtils;
import com.bixolabs.cascading.SplitterAssembly;
import com.bixolabs.cascading.TupleLogger;

@SuppressWarnings("deprecation")
public class WebMiningWorkflow {

    // Max URLs to fetch in local vs. distributed mode.
    private static final long MAX_LOCAL_FETCH = 5;
    private static final long MAX_DISTRIBUTED_FETCH = 100;
    
    @SuppressWarnings("serial")
    private static class SplitFetchedUnfetchedSSCrawlDatums extends BaseSplitter {

        @Override
        public String getLHSName() {
            return "unfetched crawl db datums";
        }

        @Override
        // LHS represents unfetched tuples
        public boolean isLHS(TupleEntry tupleEntry) {
            CrawlDbDatum datum = new CrawlDbDatum(tupleEntry);
            UrlStatus status = datum.getLastStatus();
            if (status == UrlStatus.UNFETCHED                
                            || status == UrlStatus.SKIPPED_DEFERRED
                            || status == UrlStatus.SKIPPED_BY_SCORER
                            || status == UrlStatus.SKIPPED_BY_SCORE
                            || status == UrlStatus.SKIPPED_TIME_LIMIT
                            || status == UrlStatus.SKIPPED_INTERRUPTED
                            || status == UrlStatus.SKIPPED_INEFFICIENT
                            || status == UrlStatus.ABORTED_SLOW_RESPONSE
                            || status == UrlStatus.ERROR_IOEXCEPTION) {
                return true;
            }
            return false;
        }
        
    }
    
    // TODO - why do we need this function?
    @SuppressWarnings("serial")
    private static class ConvertTextDelimitedToCrawlDbDatum extends BaseOperation<Limit.Context> implements Function<Limit.Context> {

        public ConvertTextDelimitedToCrawlDbDatum() {
            super(CrawlDbDatum.FIELDS);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Limit.Context> funcCall) {
            CrawlDbDatum datum = new CrawlDbDatum(new TupleEntry(funcCall.getArguments()));
            funcCall.getOutputCollector().add(datum.getTuple());
        }
    }

    
    @SuppressWarnings("serial")
    private static class CreateUrlDatumFromCrawlDbDatum extends BaseOperation<Limit.Context> implements Function<Limit.Context> {

        private long _limit = 0;

        public CreateUrlDatumFromCrawlDbDatum(long limit) {
            super(UrlDatum.FIELDS);
            
            _limit = limit;
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Limit.Context> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            Context context = new Context();

            operationCall.setContext( context );

            HadoopFlowProcess process = (HadoopFlowProcess)flowProcess;

            int numTasks = 0;

            if( process.isMapper() )
              numTasks = process.getCurrentNumMappers();
            else
              numTasks = process.getCurrentNumReducers();

            int taskNum = process.getCurrentTaskNum();

            context.limit = (long) Math.floor( (double) _limit / (double) numTasks );

            long remainingLimit = _limit % numTasks;

            // evenly divide limits across tasks
            context.limit += taskNum < remainingLimit ? 1 : 0;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Limit.Context> funcCall) {
            CrawlDbDatum datum = new CrawlDbDatum(funcCall.getArguments());
            UrlDatum urlDatum = new UrlDatum(datum.getUrl());
            urlDatum.setPayloadValue(CustomFields.PAGE_SCORE_FN, datum.getPageScore());
            urlDatum.setPayloadValue(CustomFields.LINKS_SCORE_FN, datum.getLinksScore());
            urlDatum.setPayloadValue(CustomFields.STATUS_FN, datum.getLastStatus().toString());
            urlDatum.setPayloadValue(CustomFields.SKIP_BY_LIMIT_FN, funcCall.getContext().increment());
            
            funcCall.getOutputCollector().add(urlDatum.getTuple());
        }
    }


    public static void importSeedUrls(Path crawlDbPath, String fileName) throws IOException, InterruptedException  {
        
        SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
        JobConf defaultJobConf = HadoopUtils.getDefaultJobConf();
        
        InputStream is = null;
        TupleEntryCollector writer = null;
        try {
            Tap urlSink = new Hfs(new TextLine(), crawlDbPath.toString(), true);
            writer = urlSink.openForWrite(defaultJobConf);

            is = WebMiningWorkflow.class.getResourceAsStream(fileName);
            if (is == null) {
                throw new FileNotFoundException("The seed urls file doesn't exist");
            }

            @SuppressWarnings("unchecked")
            List<String> lines = IOUtils.readLines(is);
            for (String line : lines) {
                line = line.trim();
                if (line.startsWith("#")) {
                    continue;
                }

                CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize(line), 0, UrlStatus.UNFETCHED, 0.0f, 0.0f);
                writer.add(datum.getTuple());
            }

            writer.close();
        } catch (IOException e) {
            HadoopUtils.safeRemove(crawlDbPath.getFileSystem(defaultJobConf), crawlDbPath);
            throw e;
        } finally {
            IoUtils.safeClose(is);
            if (writer != null) {
                writer.close();
            }
        }

    }
    
    public static Flow createFetchWorkflow(Path crawlDbPath, Path curLoopDirPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, 
                    WebMiningOptions options, boolean resetSolr) throws IOException, InterruptedException {
        
        // Fetch at most 200 pages, max size of 128K, complete mode, from the current dir.
        // HTML only. Custom useragent for Strata web mining tutorial (I can set up page on SU site)
        // e.g. "Strata web miner"
        
        // We want to extract the cleaned up HTML, and pass that to the parser, which will
        // be specified via options.getAnalyzer. From this we'll get outlinks, page score, and
        // any results.
        
        JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
        boolean isLocal = HadoopUtils.isJobLocal(conf);
        int numReducers = 1; // we always want to use a single reducer, to avoid contention
        conf.setNumReduceTasks(numReducers);
        conf.setInt("mapred.min.split.size", 64 * 1024 * 1024);
        Properties props = HadoopUtils.getDefaultProperties(WebMiningWorkflow.class, false, conf);
        FileSystem fs = crawlDbPath.getFileSystem(conf);

        // Input : the crawldb
        if (!fs.exists(crawlDbPath)) {
            throw new RuntimeException("CrawlDb not found");
        }

        Tap inputSource = new Hfs(new TextDelimited(CrawlDbDatum.FIELDS, "\t", CrawlDbDatum.TYPES), crawlDbPath.toString());
        Pipe importPipe = new Pipe("import pipe");

        // Since UrlStatus is a String, we need to convert it first.
        importPipe = new Each(importPipe, new ConvertTextDelimitedToCrawlDbDatum());
        
        // Split into tuples that are to be fetched and that have already been fetched
        SplitterAssembly splitter = new SplitterAssembly(importPipe, new SplitFetchedUnfetchedSSCrawlDatums());

        Pipe finishedDatumsFromDb = new Pipe("finished datums from db", splitter.getRHSPipe());
        Pipe urlsToFetchPipe = splitter.getLHSPipe();

        // Limit to MAX_DISTRIBUTED_FETCH if running in real cluster, 
        // or MAX_LOCAL_FETCH if running locally. So first we sort the entries 
        // from high to low by links score.
        // TODO add unit test
        urlsToFetchPipe = new GroupBy(urlsToFetchPipe, new Fields(CrawlDbDatum.LINKS_SCORE_FIELD), true);
        long maxToFetch = HadoopUtils.isJobLocal(conf) ? MAX_LOCAL_FETCH : MAX_DISTRIBUTED_FETCH;
        urlsToFetchPipe = new Each(urlsToFetchPipe, new CreateUrlDatumFromCrawlDbDatum(maxToFetch));

        BaseScoreGenerator scorer = new LinkScoreGenerator();

        // Create the sub-assembly that runs the fetch job
        int maxThreads = isLocal ? CrawlConfig.DEFAULT_NUM_THREADS_LOCAL :  CrawlConfig.DEFAULT_NUM_THREADS_CLUSTER;
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, fetcherPolicy, userAgent);
        fetcher.setMaxRetryCount(CrawlConfig.MAX_RETRIES);
        fetcher.setSocketTimeout(CrawlConfig.SOCKET_TIMEOUT);
        fetcher.setConnectionTimeout(CrawlConfig.CONNECTION_TIMEOUT);


        FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, numReducers);
        Pipe statusPipe = new Pipe("status pipe", fetchPipe.getStatusTailPipe());
        Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());
        contentPipe = TupleLogger.makePipe(contentPipe, true);

        ParseContext parseContext = new ParseContext();
        parseContext.set(HtmlMapper.class, new LowercaseIdentityHtmlMapper());
        SimpleParser parser = new SimpleParser(new HtmlContentExtractor("html"), new SimpleLinkExtractor(), new ParserPolicy(), parseContext);
        
        ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), parser);
        Pipe analyzerPipe = new Pipe("analyzer pipe");
        analyzerPipe = new Each(parsePipe.getTailPipe(), new AnalyzeHtmlFunction(options.getAnalyzer()));
        Pipe outlinksPipe = new Pipe("outlinks pipe", analyzerPipe);
        outlinksPipe = new Each(outlinksPipe, new CreateLinkDatumFromOutlinksFunction());

        String username = options.getUsername();
        
        Pipe resultsPipe = new Pipe("results pipe", analyzerPipe);
        resultsPipe = new Each(resultsPipe, new CreateResultsFunction());
        
        // Group the finished datums, the skipped datums, status, outlinks
        Pipe updatePipe = new CoGroup("update pipe", Pipe.pipes(finishedDatumsFromDb, statusPipe, analyzerPipe, outlinksPipe), 
                        Fields.fields(new Fields(CrawlDbDatum.URL_FIELD), new Fields(StatusDatum.URL_FN), 
                                        new Fields(AnalyzedDatum.URL_FIELD), new Fields(LinkDatum.URL_FN)), null, new OuterJoin());
        updatePipe = new Every(updatePipe, new UpdateCrawlDbBuffer(), Fields.RESULTS);

        
        // output : loop dir specific crawldb
        Path outCrawlDbPath = new Path(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap crawlDbSink = new Hfs(new TextLine(), outCrawlDbPath.toString());
        // Status, 
        Path statusDirPath = new Path(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusSink = new Hfs(new TextLine(), statusDirPath.toString());
        // Content
        Path contentDirPath = new Path(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
        Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS), contentDirPath.toString());
        
        // PageResults
        Path resultsDirPath = new Path(curLoopDirPath, CrawlConfig.RESULTS_SUBDIR_NAME);
        Tap resultsSink = new Hfs(new TextLine(), resultsDirPath.toString());

        // Create the output map that connects each tail pipe to the appropriate sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(updatePipe.getName(), crawlDbSink);
        sinkMap.put(statusPipe.getName(), statusSink);
        sinkMap.put(contentPipe.getName(), contentSink);
        sinkMap.put(resultsPipe.getName(), resultsSink);

        FlowConnector flowConnector = new FlowConnector(props);
        Flow flow = flowConnector.connect(inputSource, sinkMap, updatePipe, statusPipe, contentPipe, resultsPipe);

        List<FlowStep> flowSteps = flow.getSteps();
        for (FlowStep step : flowSteps) {
            step.setParentFlowName(username + ": " + getFlowStepName(step));
        }

        return flow;
    }
    
    @SuppressWarnings("rawtypes")
    private static String getFlowStepName(FlowStep step) {
        final Pattern DEFAULT_OPERATION_NAME_PATTERN = Pattern.compile("(.+)\\[.+\\]");

        Group group = step.getGroup();
        
        String stepName = "";
        if (group == null) {
            Collection<Operation> operations = step.getAllOperations();
            for (Operation operation : operations) {
                String operationName = operation.toString();
                Matcher defaultNameMatcher = DEFAULT_OPERATION_NAME_PATTERN.matcher(operationName);
                if (defaultNameMatcher.matches()) {
                    operationName = defaultNameMatcher.group(1);
                }
                stepName = stepName + operationName + "+";
            }
            
            if (operations.size() > 0) {
                stepName = stepName.substring(0, stepName.length()-1);
            }
        } else {
            stepName = group.getName();
        }
        
        return stepName;
    }

}
