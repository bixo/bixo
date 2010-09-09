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

    @SuppressWarnings("serial")
    private static class CreateCrawlDbDatumFromUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        private long _numCreated;
        
        public CreateCrawlDbDatumFromUrlFunction() {
            super(CrawlDbDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting creation of crawldb datums");
            _numCreated = 0;
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending creation of crawldb datums");
            LOGGER.info("Crawldb datums created : " + _numCreated);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            UrlDatum datum = new UrlDatum(funcCall.getArguments());
            Long lastFetched = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
            Long lastUpdated = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD);
            UrlStatus status = UrlStatus.valueOf((String)(datum.getPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD)));
            Integer crawlDepth = (Integer) datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH);

            CrawlDbDatum crawldbDatum = new CrawlDbDatum(datum.getUrl(), lastFetched, lastUpdated, status, crawlDepth);

            funcCall.getOutputCollector().add(crawldbDatum.getTuple());
            _numCreated++;
        }
    }

    
    @SuppressWarnings("serial")
    private static class CreateUrlDatumFromStatusFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        private int _numCreated;
        
        public CreateUrlDatumFromStatusFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting creation of URLs from status");
            _numCreated = 0;
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending creation of URLs from status");
            LOGGER.info(String.format("Created %d URLs", _numCreated));
        }

        // TODO VMa - verify w/Ken about this method...
        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            StatusDatum datum = new StatusDatum(funcCall.getArguments());
            UrlStatus status = datum.getStatus();
            String url = datum.getUrl();
            long statusTime = datum.getStatusTime();
            long fetchTime;

            if (status == UrlStatus.FETCHED) {
                fetchTime = statusTime;
            } else if (status == UrlStatus.SKIPPED_BY_SCORER) {
                status = UrlStatus.FETCHED;
                fetchTime = statusTime; // Not strictly true, but we need old status time passed through
                
                // TODO KKr - it would be nice to be able to get the old status here,
                // versus "knowing" that the only time a url is skipped by our scorer is
                // when it's already been fetched.
            } else if (status == UrlStatus.UNFETCHED) {
                // Since we only try to fetch URLs that have never been fetched, we know that the
                // last fetch time will always be 0.
                fetchTime = 0;
            } else {
                LOGGER.error(String.format("Unknown status %s for URL %s", status, url));
                return;
            }

            _numCreated += 1;

            UrlDatum urlDatum = new UrlDatum(url);
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, fetchTime);
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, statusTime);
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, status.name());
            // Don't change the crawl depth here - we do that only in the case of a 
            // successful parse
            urlDatum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH));

            funcCall.getOutputCollector().add(urlDatum.getTuple());
        }
    }

    @SuppressWarnings("serial")
    private static class CreateUrlDatumFromOutlinksFunction extends BaseOperation<NullContext> implements Function<NullContext> {
        private static final Logger LOGGER = Logger.getLogger(CreateUrlDatumFromOutlinksFunction.class);
        
        public CreateUrlDatumFromOutlinksFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting creation of outlink URLs");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending creation of outlink URLs");
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            ParsedDatum datum = new ParsedDatum(funcCall.getArguments());
            Outlink outlinks[] = datum.getOutlinks();
            
            // Bump the crawl depth value only on a successful parse
            int crawlDepth = (Integer)datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH);
            datum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, crawlDepth + 1);
            
            TupleEntryCollector collector = funcCall.getOutputCollector();

            for (Outlink outlink : outlinks) {
                String url = outlink.getToUrl();
                url = url.replaceAll("[\n\r]", "");
                
                UrlDatum urlDatum = new UrlDatum(url);
                urlDatum.setPayload(datum.getPayload());
                collector.add(urlDatum.getTuple());
            }
        }
    }

    @SuppressWarnings("serial")
    private static class LatestUrlDatumBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        private int _numIgnored;
        private int _numLater;
        
        public LatestUrlDatumBuffer() {
            super(UrlDatum.FIELDS);
        }
        
        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting selection of outlink URLs");
            
            _numIgnored = 0;
            _numLater = 0;
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending selection of outlink URLs");
            LOGGER.info(String.format("Ignored a total of %d duplicate URL(s) with earlier (or no) fetch time", _numIgnored));
            if (_numLater > 0) {
                LOGGER.info(String.format("Picked a total of %d URL(s) with later fetch time", _numLater));
            }
        }

        @Override
        public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
            UrlDatum bestDatum = null;
            
            int ignoredUrls = 0;
            long bestFetched = 0;
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            while (iter.hasNext()) {
                UrlDatum datum = new UrlDatum(iter.next());
                if (bestDatum == null) {
                    bestDatum = datum;
                    bestFetched = (Long)bestDatum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
                } else if ((Long)datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD) > bestFetched) {
                    if (bestFetched != 0) {
                        _numLater += 1;
                        // Should never happen that we double-fetch a page
                        LOGGER.warn("Using URL with later fetch time: " + datum.getUrl());
                    }
                    
                    // last fetched time will be 0 for never-fetched
                    bestDatum = datum;
                    bestFetched = (Long)bestDatum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
                } else {
                    ignoredUrls += 1;
                }
            }
            
            _numIgnored += ignoredUrls;
            if (ignoredUrls >= 100) {
                LOGGER.info(String.format("Ignored %d duplicate URL(s) with earlier (or no) fetch time: %s", ignoredUrls, bestDatum.getUrl()));
            }
            
            if (bestDatum != null) {
                bufferCall.getOutputCollector().add(bestDatum.getTuple());
            }
        }

    }
    
    public static Flow createFlow(Path curWorkingDirPath, Path crawlDbPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, IUrlFilter urlFilter, SimpleCrawlToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(SimpleCrawlConfig.CRAWL_STACKSIZE_KB);
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
        Path outCrawlDbPath = new Path(curWorkingDirPath, SimpleCrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap loopCrawldbSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), outCrawlDbPath.toString());

        Path contentDirPath = new Path(curWorkingDirPath, SimpleCrawlConfig.CONTENT_SUBDIR_NAME);
        Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS), contentDirPath.toString());

        Path parseDirPath = new Path(curWorkingDirPath, SimpleCrawlConfig.PARSE_SUBDIR_NAME);
        Tap parseSink = new Hfs(new SequenceFile(ParsedDatum.FIELDS), parseDirPath.toString());

        Path statusDirPath = new Path(curWorkingDirPath, SimpleCrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusSink = new Hfs(new TextLine(), statusDirPath.toString());

        // Create the sub-assembly that runs the fetch job
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(options.getMaxThreads(), fetcherPolicy, userAgent);
        fetcher.setMaxRetryCount(SimpleCrawlConfig.MAX_RETRIES);
        fetcher.setSocketTimeout(SimpleCrawlConfig.SOCKET_TIMEOUT);
        fetcher.setConnectionTimeout(SimpleCrawlConfig.CONNECTION_TIMEOUT);

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
