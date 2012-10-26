/*
 * Copyright 2009-2012 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.examples.crawl;

import bixo.config.FetcherPolicy;
import bixo.config.ParserPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.operations.NormalizeUrlFunction;
import bixo.operations.UrlFilter;
import bixo.parser.SimpleLinkExtractor;
import bixo.parser.SimpleParser;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParsePipe;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import bixo.urls.SimpleUrlValidator;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.scheme.WritableSequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.bixolabs.cascading.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.*;


@SuppressWarnings("deprecation")
public class NotSoSimpleCrawlWorkflow {

    private static final Logger LOGGER = Logger.getLogger(NotSoSimpleCrawlWorkflow.class);


    @SuppressWarnings("serial")
    private static class SplitFetchedUnfetchedCrawlDatums extends BaseSplitter {

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


    public static Flow createFlow(Path curWorkingDirPath, Path crawlDbPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, BaseUrlFilter urlFilter, NotSoSimpleCrawlToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
        int numReducers = HadoopUtils.getNumReducers(conf);
        conf.setNumReduceTasks(numReducers);
        Properties props = HadoopUtils.getDefaultProperties(NotSoSimpleCrawlWorkflow.class, options.isDebugLogging(), conf);
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
        // A TupleLogger is a good way to follow the tuples around in a flow. You can enable the output
        // of tuples by setting options.setDebugLogging() to true.
        urlsToFetchPipe = TupleLogger.makePipe(urlsToFetchPipe, true);

        // Create the output sinks :
        //      crawldb
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

        Path mahoutDirPath = new Path(curWorkingDirPath, CrawlConfig.MAHOUT_SUBDIR_NAME);
        Tap mahoutSink = new Hfs( new WritableSequenceFile(new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN), Text.class, Text.class ), mahoutDirPath.toString());

        // Create the sub-assembly that runs the fetch job
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(options.getMaxThreads(), fetcherPolicy, userAgent);
        fetcher.setMaxRetryCount(CrawlConfig.MAX_RETRIES);
        fetcher.setSocketTimeout(CrawlConfig.SOCKET_TIMEOUT);
        fetcher.setConnectionTimeout(CrawlConfig.CONNECTION_TIMEOUT);

        // You can also provide a set of mime types you want to restrict what content type you 
        // want to deal with - for now keep it simple.
        Set<String> validMimeTypes = new HashSet<String>();
        validMimeTypes.add("text/plain");
        validMimeTypes.add("text/html");
        fetcherPolicy.setValidMimeTypes(validMimeTypes);

        // The scorer is used by the FetchPipe to assign a score to every URL that passes the 
        // robots.txt processing. The score is used to sort URLs such that higher scoring URLs
        // are fetched first. If URLs are skipped for any reason(s) lower scoring URLs are skipped.
        BaseScoreGenerator scorer = new FixedScoreGenerator();

        FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, numReducers);
        Pipe statusPipe = new Pipe("status pipe", fetchPipe.getStatusTailPipe());
        Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());
        contentPipe = TupleLogger.makePipe(contentPipe, true);


        // Take content and split it into content output plus parse to extract URLs.
        //SimpleParser parser = new SimpleParser();
        //parser.setExtractLanguage(false);
        //ParsePipe parsePipe = new ParsePipe(contentPipe, parser);
        SimpleParser parser;
        if(options.getUseBoilerpipe()){
            parser = new SimpleParser(new BoilerpipeContentExtractor(), new SimpleLinkExtractor(), new ParserPolicy());
        } else {
            parser = new SimpleParser();
        }

        if(!options.getLanguageName().equals("")){
            parser.setExtractLanguage(true);
        } else {
            parser.setExtractLanguage(false);
        }
        ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), parser);

        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlDatumFromOutlinksFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator()));
        if (urlFilter != null) {
            urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        }
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer()));
        urlFromOutlinksPipe = TupleLogger.makePipe(urlFromOutlinksPipe, true);


        // Take status and output urls from it  
        Pipe urlFromFetchPipe = new Pipe("url from fetch");
        urlFromFetchPipe = new Each(statusPipe, new CreateUrlDatumFromStatusFunction());
        urlFromFetchPipe = TupleLogger.makePipe(urlFromFetchPipe, true);

        // Finally join the URLs we get from parsing content with the URLs we got
        // from the status ouput, and the urls we didn't process from the db so that 
        // we have a unified stream of all known URLs for the crawldb.
        Pipe finishedUrlsFromDbPipe = new Each(finishedDatumsFromDb, new CreateUrlDatumFromCrawlDbFunction());
        finishedUrlsFromDbPipe = TupleLogger.makePipe(finishedUrlsFromDbPipe, true);

        // NOTE : Ideally you would just do a CoGroup instead of converting all the pipes to emit UrlDatums 
        // and then doing the extra step of converting from UrlDatum to CrawlDbDatum.
        // The reason this isn't being done here is because we are sharing LatestUrlDatumBuffer() with JDBCCrawlTool
        Pipe crawlDbPipe = new GroupBy("crawldb pipe", Pipe.pipes(urlFromFetchPipe, urlFromOutlinksPipe, finishedUrlsFromDbPipe),
                new Fields(UrlDatum.URL_FN));
        crawlDbPipe = new Every(crawlDbPipe, new LatestUrlDatumBuffer(), Fields.RESULTS);

        Pipe outputPipe = new Pipe ("output pipe");
        outputPipe = new Each(crawlDbPipe, new CreateCrawlDbDatumFromUrlFunction());

        //create the Mahout output pipe, this will turn the cascading tuple into a hadoop sequence file
        //currently uses the URL for the key and the Title + Text as content
        Pipe mahoutExporterPipe = new Pipe("output parsed text to mahout sequence file", parsePipe.getTailPipe());
        WriteMahoutSequenceFileFunction wf = new WriteMahoutSequenceFileFunction();
        if(!options.getLanguageName().equals("")){
            wf.setlanguageName(options.getLanguageName());//this actually looks to see if the parsed language string contain this string, so a loose interpretation.
        }
        mahoutExporterPipe = new Each(mahoutExporterPipe, wf);

        // Create the output map that connects each tail pipe to the appropriate sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(statusPipe.getName(), statusSink);
        sinkMap.put(contentPipe.getName(), contentSink);
        sinkMap.put(ParsePipe.PARSE_PIPE_NAME, parseSink);
        sinkMap.put(crawlDbPipe.getName(), loopCrawldbSink);
        sinkMap.put(mahoutExporterPipe.getName(), mahoutSink);

        FlowConnector flowConnector = new FlowConnector(props);
        Flow flow = flowConnector.connect(inputSource, sinkMap, statusPipe, contentPipe, parsePipe.getTailPipe(), outputPipe, mahoutExporterPipe );

        return flow;
    }

}
