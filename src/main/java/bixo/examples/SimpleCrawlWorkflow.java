package bixo.examples;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import bixo.cascading.Splitter;
import bixo.cascading.NullContext;
import bixo.cascading.SplitterAssembly;
import bixo.cascading.TupleLogger;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.FixedScoreGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.hadoop.HadoopUtils;
import bixo.operations.NormalizeUrlFunction;
import bixo.operations.UrlFilter;
import bixo.parser.SimpleParser;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParsePipe;
import bixo.url.IUrlFilter;
import bixo.url.SimpleUrlNormalizer;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;


public class SimpleCrawlWorkflow {

    private static final Logger LOGGER = Logger.getLogger(SimpleCrawlWorkflow.class);


    @SuppressWarnings("serial")
    private static class SplitFetchedUnfetchedCrawlDatums extends Splitter {

        @Override
        public String getLHSName() {
            return "fetched unfetched UrlDatums";
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

    @SuppressWarnings("serial")
    private static class CreateUrlDatumFromCrawlDbFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateUrlDatumFromCrawlDbFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting creation of URLs from crawldb");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending creation of URLs from status");
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            CrawlDbDatum datum = new CrawlDbDatum(funcCall.getArguments());
            UrlDatum urlDatum = new UrlDatum(datum.getUrl());
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, datum.getLastFetched());
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, datum.getLastUpdated());
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, datum.getLastStatus().name());
            urlDatum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, datum.getCrawlDepth());
            
            funcCall.getOutputCollector().add(urlDatum.getTuple());
        }
    }


    

 
    
    public static Flow createFlow(Path curWorkingDirPath, Path crawlDbPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, IUrlFilter urlFilter, SimpleCrawlToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
        int numReducers = conf.getNumReduceTasks() * HadoopUtils.getTaskTrackers(conf);
        Properties props = HadoopUtils.getDefaultProperties(SimpleCrawlWorkflow.class, options.isDebugLogging(), conf);
        FileSystem fs = curWorkingDirPath.getFileSystem(conf);

        // Input : the crawldb
        if (!fs.exists(crawlDbPath)) {
            throw new RuntimeException("CrawlDb not found");
        }

        // Our crawl db is defined by the CrawlDbDatum
        Tap inputSource = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toString());
        Pipe importPipe = new Pipe("import pipe");

        // Split into tuples that are to be fetched and that have already been fetched
        SplitterAssembly splitter = new SplitterAssembly(importPipe, new SplitFetchedUnfetchedCrawlDatums());

        Pipe finishedDatumsFromDb = splitter.getRHSPipe();
        Pipe urlsToFetchPipe = new Pipe("urls to Fetch", splitter.getLHSPipe());

        // Convert the urlsToFetchPipe so that we now deal with UrlDatums.
        urlsToFetchPipe = new Each(urlsToFetchPipe, new CreateUrlDatumFromCrawlDbFunction());
        urlsToFetchPipe = TupleLogger.makePipe(urlsToFetchPipe, true);

        // Create the output sinks :
        //      loop dir crawldb
        //      content
        //      parse
        //      status
        Path outCrawlDbPath = new Path(curWorkingDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap loopCrawldbSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), outCrawlDbPath.toString());

        Path contentDirPath = new Path(curWorkingDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
        Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS), contentDirPath.toString());

        Path parseDirPath = new Path(curWorkingDirPath, CrawlConfig.PARSE_SUBDIR_NAME);
        Tap parseSink = new Hfs(new SequenceFile(ParsedDatum.FIELDS), parseDirPath.toString());

        Path statusDirPath = new Path(curWorkingDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusSink = new Hfs(new TextLine(), statusDirPath.toString());

        // Create the sub-assembly that runs the fetch job
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(options.getMaxThreads(), fetcherPolicy, userAgent);
        fetcher.setMaxRetryCount(CrawlConfig.MAX_RETRIES);
        fetcher.setSocketTimeout(CrawlConfig.SOCKET_TIMEOUT);
        fetcher.setConnectionTimeout(CrawlConfig.CONNECTION_TIMEOUT);

        // You can also provide a set of mime types you want to deal with - for now keep it simple.
        Set<String> validMimeTypes = new HashSet<String>();
        validMimeTypes.add("text/plain");
        validMimeTypes.add("text/html");
        fetcherPolicy.setValidMimeTypes(validMimeTypes);

        ScoreGenerator scorer = new FixedScoreGenerator();

        FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, numReducers);
        Pipe statusPipe = new Pipe("status pipe", fetchPipe.getStatusTailPipe());
        Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());
        contentPipe = TupleLogger.makePipe(contentPipe, true);

        
        // Take content and split it into content output plus parse to extract URLs.
        SimpleParser parser = new SimpleParser();
        parser.setExtractLanguage(false);
        ParsePipe parsePipe = new ParsePipe(contentPipe, parser);

        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlDatumFromOutlinksFunction());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer()));
        urlFromOutlinksPipe = TupleLogger.makePipe(urlFromOutlinksPipe, true);


        // Take status and output urls from it  
        Pipe urlFromFetchPipe = new Pipe("url from fetch");
        urlFromFetchPipe = new Each(statusPipe, new CreateUrlDatumFromStatusFunction());
        urlFromFetchPipe = TupleLogger.makePipe(urlFromFetchPipe, true);

        // Finally join the URLs we get from parsing content with the URLs we got
        // from the status ouput, and the urls we didn't process from the db so  that 
        // we have a unified stream of all known URLs for the crawldb.
        Pipe finishedUrlsFromDbPipe = new Each(finishedDatumsFromDb, new CreateUrlDatumFromCrawlDbFunction());
        finishedUrlsFromDbPipe = TupleLogger.makePipe(finishedUrlsFromDbPipe, true);

        // NOTE : Ideally you would just do a CoGroup instead of converting all the pipes to emit UrlDatums
        Pipe crawlDbPipe = new GroupBy("crawldb pipe", Pipe.pipes(urlFromFetchPipe, urlFromOutlinksPipe, finishedUrlsFromDbPipe), new Fields(UrlDatum.URL_FN));
        crawlDbPipe = new Every(crawlDbPipe, new LatestUrlDatumBuffer(), Fields.RESULTS);
        
        Pipe outputPipe = new Pipe ("output pipe");
        outputPipe = new Each(crawlDbPipe, new CreateCrawlDbDatumFromUrlFunction());
        
        // Create the output map that connects each tail pipe to the appropriate sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(statusPipe.getName(), statusSink);
        sinkMap.put(contentPipe.getName(), contentSink);
        sinkMap.put(ParsePipe.PARSE_PIPE_NAME, parseSink);
        sinkMap.put(crawlDbPipe.getName(), loopCrawldbSink);

        FlowConnector flowConnector = new FlowConnector(props);
        Flow flow = flowConnector.connect(inputSource, sinkMap, statusPipe, contentPipe, parsePipe.getTailPipe(), outputPipe);

        return flow;
    }

}
