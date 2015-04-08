/*
 * Copyright 2009-2015 Scale Unlimited
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.ParserPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.operations.UrlFilter;
import bixo.parser.BoilerpipeContentExtractor;
import bixo.parser.HtmlContentExtractor;
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
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.operation.regex.RegexReplace;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BaseSplitter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.SplitterAssembly;
import com.scaleunlimited.cascading.TupleLogger;



public class DemoCrawlWorkflow {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoCrawlWorkflow.class);


    @SuppressWarnings("serial")
    private static class SplitFetchedUnfetchedCrawlDatums extends BaseSplitter {

        @Override
        public String getLHSName() {
            return "unfetched UrlDatums";
        }

        @Override
        // LHS represents unfetched tuples
        public boolean isLHS(TupleEntry tupleEntry) {
            CrawlDbDatum datum = new CrawlDbDatum(tupleEntry);
            return CrawlConfig.isUnfetchedStatus(datum.getLastStatus());
        }
    }

    @SuppressWarnings({"serial", "rawtypes"})
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

    
    @SuppressWarnings("rawtypes")
    public static Flow createFlow(BasePath curWorkingDirPath, BasePath crawlDbPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, BaseUrlFilter urlFilter, DemoCrawlToolOptions options) throws Throwable {

        BixoPlatform platform = new BixoPlatform(DemoCrawlWorkflow.class, options.getPlatformMode());
        platform.setNumReduceTasks(options.getNumReduceTasks());

        // Input : the crawldb
        crawlDbPath.assertExists("CrawlDb doesn't exist");

        // Our crawl db is defined by the CrawlDbDatum
        Tap inputSource = platform.makeTap(platform.makeBinaryScheme(CrawlDbDatum.FIELDS), crawlDbPath);
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
        BasePath outCrawlDbPath = platform.makePath(curWorkingDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap loopCrawldbSink = platform.makeTap(platform.makeBinaryScheme(CrawlDbDatum.FIELDS), outCrawlDbPath, SinkMode.REPLACE);

        BasePath contentDirPath = platform.makePath(curWorkingDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
        Tap contentSink = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), contentDirPath, SinkMode.REPLACE);

        BasePath parseDirPath = platform.makePath(curWorkingDirPath, CrawlConfig.PARSE_SUBDIR_NAME);
        Tap parseSink = platform.makeTap(platform.makeBinaryScheme(ParsedDatum.FIELDS), parseDirPath, SinkMode.REPLACE);

        BasePath statusDirPath = platform.makePath(curWorkingDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusSink = platform.makeTap(platform.makeTextScheme(), statusDirPath, SinkMode.REPLACE);

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

        FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, platform.getNumReduceTasks());
        Pipe statusPipe = new Pipe("status pipe", fetchPipe.getStatusTailPipe());
        Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());
        contentPipe = TupleLogger.makePipe(contentPipe, true);
        
        // Take content and split it into content output plus parse to extract URLs.
        SimpleParser parser;
        if (options.isUseBoilerpipe()) {
            parser = new SimpleParser(new BoilerpipeContentExtractor(), new SimpleLinkExtractor(), new ParserPolicy());
        } else if (options.isGenerateHTML()) {
            parser = new SimpleParser(new HtmlContentExtractor(), new SimpleLinkExtractor(), new ParserPolicy(), true);
        } else {
            parser = new SimpleParser();
        }
        
        parser.setExtractLanguage(false);
        ParsePipe parsePipe = new ParsePipe(contentPipe, parser);

        
        // Create the output map that connects each tail pipe to the appropriate sink, and the
        // list of tail pipes.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        List<Pipe> tailPipes = new ArrayList<Pipe>();
        
        if (options.isGenerateHTML()) {
            // Let's write out the parse as text:
            Pipe textParsePipe = new Pipe("text parse data", parsePipe.getTailPipe());
            textParsePipe = new Each(textParsePipe, new Fields(ParsedDatum.PARSED_TEXT_FN), new RegexReplace(new Fields(ParsedDatum.PARSED_TEXT_FN), "[\\r\\n\\t]+", " ", true), Fields.REPLACE);
            textParsePipe = new Each(textParsePipe, new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN), new Identity());
            BasePath textParsePath = platform.makePath(curWorkingDirPath, CrawlConfig.HTML_SUBDIR_NAME);
            Tap textParseTap = platform.makeTap(platform.makeTextScheme(), textParsePath, SinkMode.REPLACE);
            sinkMap.put(textParsePipe.getName(), textParseTap);
            tailPipes.add(textParsePipe);
        }
        
        // Let's output a WritableSequenceFile as an example - this file can
        // then be used as input when working with Mahout.
        // For now we only do it when we are running in Hadoop mode
          Tap writableSeqFileSink = null;
          Pipe writableSeqFileDataPipe = null;
            if (!options.isLocalPlatformMode()) {
                writableSeqFileDataPipe = new Pipe("writable seqfile data", new Each(parsePipe.getTailPipe(), new CreateWritableSeqFileData()));
                BasePath writableSeqFileDataPath = platform.makePath(curWorkingDirPath, CrawlConfig.EXTRACTED_TEXT_SUBDIR_NAME);
                WritableSequenceFile writableSeqScheme = new WritableSequenceFile(new Fields(CrawlConfig.WRITABLE_SEQ_FILE_KEY_FN, CrawlConfig.WRITABLE_SEQ_FILE_VALUE_FN), Text.class, Text.class);
                writableSeqFileSink = platform.makeTap(writableSeqScheme, writableSeqFileDataPath, SinkMode.REPLACE);
            }
        
        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlDatumFromOutlinksFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator()));
        if (urlFilter != null) {
            urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        }
        
        urlFromOutlinksPipe = TupleLogger.makePipe(urlFromOutlinksPipe, true);

        // Take status and output urls from it  
        Pipe urlFromFetchPipe = new Pipe("url from fetch", statusPipe);
        urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlDatumFromStatusFunction());
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
        
        // Create the output map that connects each tail pipe to the appropriate sink.
        sinkMap.put(statusPipe.getName(), statusSink);
        tailPipes.add(statusPipe);
        
        sinkMap.put(contentPipe.getName(), contentSink);
        tailPipes.add(contentPipe);

        sinkMap.put(parsePipe.getTailPipe().getName(), parseSink);
        tailPipes.add(parsePipe.getTailPipe());

        sinkMap.put(outputPipe.getName(), loopCrawldbSink);
        tailPipes.add(outputPipe);

        if (!options.isLocalPlatformMode()) {
            sinkMap.put(writableSeqFileDataPipe.getName(), writableSeqFileSink);
            tailPipes.add(writableSeqFileDataPipe);
        }
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(inputSource, sinkMap, tailPipes.toArray(new Pipe[tailPipes.size()]));

        return flow;
    }

}
