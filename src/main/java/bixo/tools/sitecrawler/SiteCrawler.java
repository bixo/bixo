package bixo.tools.sitecrawler;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.IScoreGenerator;
import bixo.fetcher.util.SimpleGroupingKeyGenerator;
import bixo.operations.NormalizeUrlFunction;
import bixo.operations.UrlFilter;
import bixo.parser.SimpleParser;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParsePipe;
import bixo.urldb.IUrlFilter;
import bixo.urldb.SimpleUrlNormalizer;
import bixo.utils.HadoopUtils;
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
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class SiteCrawler {
	private static final Logger LOGGER = Logger.getLogger(SiteCrawler.class);
	private static final int CRAWL_STACKSIZE_KB = 128;
	
    @SuppressWarnings("serial")
	private static class SkipFetchedScoreGenerator implements IScoreGenerator {

		@Override
		public double generateScore(GroupedUrlDatum datum) {
			if (datum.getLastFetched() != 0) {
				return IScoreGenerator.SKIP_URL_SCORE;
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
	private String _agentName;
	private FetcherPolicy _fetcherPolicy;
	private int _maxThreads;
	private IUrlFilter _urlFilter;
	
	public SiteCrawler(Path inputDir, Path outputDir, String agentName, FetcherPolicy fetcherPolicy, int maxThreads, IUrlFilter urlFilter) {
		_inputDir = inputDir;
		_outputDir = outputDir;
		_agentName = agentName;
		_fetcherPolicy = fetcherPolicy;
		_maxThreads = maxThreads;
		_urlFilter = urlFilter;
	}
	
	public void crawl(Boolean debug) throws Throwable {
		JobConf conf = HadoopUtils.getDefaultJobConf(CRAWL_STACKSIZE_KB);
		FileSystem fs = _outputDir.getFileSystem(conf);

		if (!fs.exists(_inputDir)) {
        	throw new IllegalStateException(String.format("Input directory %s doesn't exist", _inputDir));
		}

		Path urlPath = new Path(_inputDir, "urls");
        if (!fs.exists(urlPath)) {
        	throw new IllegalStateException(String.format("Input directory %s doesn't contain a \"urls\" sub-directory", _inputDir));
        }
        
        Tap inputSource = new Hfs(new SequenceFile(UrlDatum.FIELDS.append(MetaData.FIELDS)), urlPath.toString());
        Pipe importPipe = new Each("url importer", new Identity());

		try {
			String curCrawlDirName = _outputDir.toUri().toString();

			Tap statusSink = new Hfs(new TextLine(StatusDatum.FIELDS.size()), curCrawlDirName + "/status");
            Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS.append(MetaData.FIELDS)), curCrawlDirName + "/content");
            Tap parseSink = new Hfs(new SequenceFile(ParsedDatum.FIELDS.append(MetaData.FIELDS)), curCrawlDirName + "/parse");
			Tap urlSink = new Hfs(new SequenceFile(UrlDatum.FIELDS.append(MetaData.FIELDS)), curCrawlDirName + "/urls");

			// Create the sub-assembly that runs the fetch job
			SimpleGroupingKeyGenerator grouper = new SimpleGroupingKeyGenerator(_agentName);
			IScoreGenerator scorer = new SkipFetchedScoreGenerator();
			IHttpFetcher fetcher = new SimpleHttpFetcher(_maxThreads, _fetcherPolicy, _agentName);
			FetchPipe fetchPipe = new FetchPipe(importPipe, grouper, scorer, fetcher, MetaData.FIELDS);

			// Take content and split it into content output plus parse to extract URLs.
			ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), new SimpleParser(), MetaData.FIELDS);
			Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlFromOutlinksFunction());
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(_urlFilter, MetaData.FIELDS));
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer(), MetaData.FIELDS));
			
			// Take status and split it into status output plus updated UrlDatum's in the /urls sub-dir.
			Pipe urlFromFetchPipe = new Pipe("url from fetch", fetchPipe.getStatusTailPipe());
			urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlFromStatusFunction());

			// Now we need to join the URLs we get from parsing content with the URLs we got
			// from the status ouput, so we have a unified stream of all known URLs.
			Pipe urlPipe = new GroupBy("url pipe", Pipe.pipes(urlFromFetchPipe, urlFromOutlinksPipe), new Fields(UrlDatum.URL_FIELD));
			urlPipe = new Every(urlPipe, new LatestUrlBuffer(), Fields.RESULTS);

			// Create the output map that connects each tail pipe to the appropriate sink.
			Map<String, Tap> sinkMap = new HashMap<String, Tap>();
			sinkMap.put(FetchPipe.CONTENT_PIPE_NAME, contentSink);
			sinkMap.put(ParsePipe.PARSE_PIPE_NAME, parseSink);
			sinkMap.put(FetchPipe.STATUS_PIPE_NAME, statusSink);
			sinkMap.put(urlPipe.getName(), urlSink);

			// Finally we can run it.
			FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(SiteCrawler.class, debug, conf));
			Flow flow = flowConnector.connect(inputSource, sinkMap, fetchPipe, urlPipe);
			flow.complete();
		} catch (Throwable t) {
			HadoopUtils.safeRemove(fs, _outputDir);
			throw t;
		}
	}
	
}
