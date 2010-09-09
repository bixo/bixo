package bixo.examples;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.http.HttpFetcher;
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
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class JDBCCrawlWorkflow {
    private static final Logger LOGGER = Logger.getLogger(JDBCCrawlWorkflow.class);

    @SuppressWarnings("serial")
    private static class BestUrlToFetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        private long _numSelected = 0;
        
        public BestUrlToFetchBuffer() {
            super(UrlDatum.FIELDS);
        }
        
        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting selection of best URLs to fetch");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending selection of best URLs to fetch - selected " + _numSelected + " urls");
        }

        @Override
        public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
            CrawlDbDatum bestDatum = null;
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            while (iter.hasNext()) {
                CrawlDbDatum datum = new CrawlDbDatum(iter.next());
                if (bestDatum == null) {
                    bestDatum = datum;
                } else if (datum.getLastFetched() > bestDatum.getLastFetched()) {
                    bestDatum = datum;
                }    
            }
            
            if (bestDatum != null && bestDatum.getLastFetched() == 0) {
                UrlDatum urlDatum = new UrlDatum(bestDatum.getUrl());
                urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, bestDatum.getLastFetched());
                urlDatum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, bestDatum.getLastUpdated());
                urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, bestDatum.getLastStatus().name());
                urlDatum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, bestDatum.getCrawlDepth());
                
                bufferCall.getOutputCollector().add(urlDatum.getTuple());
                _numSelected++;
            }
        }

    }

    
    public static Flow createFlow(Path inputDir, Path outputDir, UserAgent userAgent, FetcherPolicy fetcherPolicy,
                    IUrlFilter urlFilter, int maxThreads, boolean debug, String persistentDbLocation) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
        int numReducers = conf.getNumReduceTasks() * HadoopUtils.getTaskTrackers(conf);
        FileSystem fs = outputDir.getFileSystem(conf);

        if (!fs.exists(inputDir)) {
            throw new IllegalStateException(String.format("Input directory %s doesn't exist", inputDir));
        }

        Tap inputSource = JDBCTapFactory.createUrlsSourceJDBCTap(persistentDbLocation);

        // Read _everything_ in initially
        // Split that pipe into URLs we want to fetch for the fetch pipe
        Pipe importPipe = new Pipe("url importer");
        importPipe = new GroupBy(importPipe, new Fields(CrawlDbDatum.URL_FIELD));
        importPipe = new Every(importPipe, new BestUrlToFetchBuffer(), Fields.RESULTS);

        String curCrawlDirName = outputDir.toUri().toString();

        Path contentPath = new Path(outputDir, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS), contentPath.toString());
        Path parsePath = new Path(outputDir, CrawlConfig.PARSE_SUBDIR_NAME);
        Tap parseSink = new Hfs(new SequenceFile(ParsedDatum.FIELDS), parsePath.toString());

        // VMa : The source and sink for urls is essentially the same database -
        // since cascading
        // doesn't allow you to use the same tap for source and sink we fake it
        // by creating
        // two separate taps.
        Tap urlSink = JDBCTapFactory.createUrlsSinkJDBCTap(persistentDbLocation);

        // Create the sub-assembly that runs the fetch job
        HttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, fetcherPolicy, userAgent);
        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(importPipe, scorer, fetcher, numReducers);

        // Take content and split it into content output plus parse to extract
        // URLs.
        ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), new SimpleParser());
        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlDatumFromOutlinksFunction());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer()));

        // Take status and output updated UrlDatum's. Again, since we are using
        // the same database
        // we need to create a new tap.
        Pipe urlFromFetchPipe = new Pipe("url from fetch", fetchPipe.getStatusTailPipe());
        urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlDatumFromStatusFunction());

        // Now we need to join the URLs we get from parsing content with the
        // URLs we got
        // from the status output, so we have a unified stream of all known
        // URLs.
        Pipe urlPipe = new GroupBy("url pipe", Pipe.pipes(urlFromFetchPipe, urlFromOutlinksPipe), new Fields(UrlDatum.URL_FN));
        urlPipe = new Every(urlPipe, new LatestUrlDatumBuffer(), Fields.RESULTS);

        // Create the output map that connects each tail pipe to the appropriate
        // sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(FetchPipe.CONTENT_PIPE_NAME, contentSink);
        sinkMap.put(ParsePipe.PARSE_PIPE_NAME, parseSink);
        sinkMap.put(urlPipe.getName(), urlSink);
        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(JDBCCrawlWorkflow.class, debug, conf));
        return flowConnector.connect(inputSource, sinkMap, fetchPipe.getContentTailPipe(), parsePipe.getTailPipe(), urlPipe);
            
    }

}
