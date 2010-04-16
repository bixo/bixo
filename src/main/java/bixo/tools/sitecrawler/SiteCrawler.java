package bixo.tools.sitecrawler;

import java.util.HashMap;
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
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.FixedScoreGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.hadoop.HadoopUtils;
import bixo.operations.NormalizeUrlFunction;
import bixo.operations.UrlFilter;
import bixo.parser.SimpleParser;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParsePipe;
import bixo.urldb.IUrlFilter;
import bixo.urldb.SimpleUrlNormalizer;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class SiteCrawler {
    private static final Logger LOGGER = Logger.getLogger(SiteCrawler.class);
    private static final int CRAWL_STACKSIZE_KB = 128;
	
    @SuppressWarnings("serial")
    private static class SkipFetchedScoreGenerator extends ScoreGenerator {
        private transient long _lastFetchedTime;
        
        @Override
        public double generateScore(String domain, String pld, GroupedUrlDatum urlDatum) {
            _lastFetchedTime = urlDatum.getLastFetched() ;
            return generateScore(domain, pld, urlDatum.getUrl());
        }
        
        @Override
        public double generateScore(String domain, String pld, String url) {
            if (_lastFetchedTime != 0) {
                return ScoreGenerator.SKIP_SCORE;
            } else {
                return 1.0;
            }
        }
        
    }

    @SuppressWarnings("serial")
    private static class CreateUrlFromStatusFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        private int _numCreated;
        
        public CreateUrlFromStatusFunction() {
            super(UrlDatum.FIELDS.append(MetaData.FIELDS));
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


        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            StatusDatum datum = new StatusDatum(funcCall.getArguments(), MetaData.FIELDS);
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
            UrlDatum urlDatum = new UrlDatum(url, fetchTime, statusTime, status, datum.getMetaDataMap());
            funcCall.getOutputCollector().add(urlDatum.toTuple());
        }
    }

	private Path _inputDir;
	private Path _outputDir;
	private UserAgent _userAgent;
	private FetcherPolicy _fetcherPolicy;
	private int _maxThreads;
	private IUrlFilter _urlFilter;
    private long _crawlEndTime;
	
	public SiteCrawler(Path inputDir, Path outputDir, UserAgent userAgent, FetcherPolicy fetcherPolicy, int maxThreads, IUrlFilter urlFilter, long crawlEndTime) {
		_inputDir = inputDir;
		_outputDir = outputDir;
		_userAgent = userAgent;
		_fetcherPolicy = fetcherPolicy;
		_maxThreads = maxThreads;
		_urlFilter = urlFilter;
		_crawlEndTime = crawlEndTime;
	}
	
	public void crawl(Boolean debug) throws Throwable {
		JobConf conf = HadoopUtils.getDefaultJobConf(CRAWL_STACKSIZE_KB);
		int numReducers = conf.getNumReduceTasks() * HadoopUtils.getTaskTrackers(conf);
		FileSystem fs = _outputDir.getFileSystem(conf);

		if (!fs.exists(_inputDir)) {
        	throw new IllegalStateException(String.format("Input directory %s doesn't exist", _inputDir));
		}

		Tap inputSource = BixoJDBCTapFactory.createUrlsSourceJDBCTap();

		// Read _everything_ in initially
        // Split that pipe into URLs we want to fetch for the fetch pipe
        Pipe importPipe = new Each("url importer", new Identity());
        Pipe fetchUrlsImportPipe = new GroupBy(importPipe, new Fields(UrlDatum.URL_FIELD));
        fetchUrlsImportPipe = new Every(fetchUrlsImportPipe, new BestUrlToFetchBuffer(), Fields.RESULTS);

		try {
			String curCrawlDirName = _outputDir.toUri().toString();

            Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS.append(MetaData.FIELDS)), curCrawlDirName + "/content");
            Tap parseSink = new Hfs(new SequenceFile(ParsedDatum.FIELDS.append(MetaData.FIELDS)), curCrawlDirName + "/parse");

            // VMa : The source and sink for urls is essentially the same database - since cascading 
            // doesn't allow you to use the same tap for source and sink we fake it by creating 
            // two separate taps.
            Tap urlSink = BixoJDBCTapFactory.createUrlsSinkJDBCTap();

			// Create the sub-assembly that runs the fetch job
			IHttpFetcher fetcher = new SimpleHttpFetcher(_maxThreads, _fetcherPolicy, _userAgent);
			ScoreGenerator scorer = new FixedScoreGenerator();
			FetchPipe fetchPipe = new FetchPipe(importPipe, scorer, fetcher, FetcherPolicy.NO_CRAWL_END_TIME, numReducers, MetaData.FIELDS);

			// Take content and split it into content output plus parse to extract URLs.
			ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), new SimpleParser(), MetaData.FIELDS);
			Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlFromOutlinksFunction());
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(_urlFilter, MetaData.FIELDS));
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer(), MetaData.FIELDS));
			
			// Take status and output updated UrlDatum's. Again, since we are using the same database 
			// we need to create a new tap.
			Pipe urlFromFetchPipe = new Pipe("url from fetch", fetchPipe.getStatusTailPipe());
			urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlFromStatusFunction());

		    // Now we need to join the URLs we get from parsing content with the URLs we got
		    // from the status output, so we have a unified stream of all known URLs.
		    Pipe urlPipe = new GroupBy("url pipe", Pipe.pipes(urlFromFetchPipe, urlFromOutlinksPipe), new Fields(UrlDatum.URL_FIELD));
		    urlPipe = new Every(urlPipe, new LatestUrlBuffer(), Fields.RESULTS);


			// Create the output map that connects each tail pipe to the appropriate sink.
			Map<String, Tap> sinkMap = new HashMap<String, Tap>();
			sinkMap.put(FetchPipe.CONTENT_PIPE_NAME, contentSink);
			sinkMap.put(ParsePipe.PARSE_PIPE_NAME, parseSink);
            sinkMap.put(urlPipe.getName(), urlSink);
			// Finally we can run it.
			FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(SiteCrawler.class, debug, conf));
            Flow flow = flowConnector.connect(inputSource, sinkMap, fetchPipe.getContentTailPipe(), parsePipe.getTailPipe(), urlPipe);
			flow.complete();
			
//			 flow.writeDOT("build/valid-flow.dot");
		} catch (Throwable t) {
			HadoopUtils.safeRemove(fs, _outputDir);
			throw t;
		}
	}


}
