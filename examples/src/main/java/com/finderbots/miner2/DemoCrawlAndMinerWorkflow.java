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
package com.finderbots.miner2;

import bixo.config.FetcherPolicy;
import bixo.config.ParserPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.operations.NormalizeUrlFunction;
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
import cascading.operation.*;
import cascading.operation.regex.RegexReplace;
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
public class DemoCrawlAndMinerWorkflow {

    private static final Logger LOGGER = Logger.getLogger(DemoCrawlAndMinerWorkflow.class);


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


    public static Flow createFlow(Path curWorkingDirPath, Path crawlDbPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, BaseUrlFilter urlFilter, AnalyzeHtml analyzer, DemoCrawlAndMinerToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
        int numReducers = HadoopUtils.getNumReducers(conf);
        conf.setNumReduceTasks(numReducers);
        Properties props = HadoopUtils.getDefaultProperties(DemoCrawlAndMinerWorkflow.class, options.isDebugLogging(), conf);
        FileSystem fs = curWorkingDirPath.getFileSystem(conf);

        // Input : the crawldb
        if (!fs.exists(crawlDbPath)) {
            throw new RuntimeException("CrawlDb doesn't exist at " + crawlDbPath);
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
        // BEWARE: The SimpleParser will discard HTML unless you pass in true as last arg! So for mining
        // always pass in true!!!
        SimpleParser parser;
        if (options.isUseBoilerpipe()) {
            parser = new SimpleParser(new BoilerpipeContentExtractor(), new SimpleLinkExtractor(), new ParserPolicy());
        } else if (options.isGenerateHTML()) {
            parser = new SimpleParser(new HtmlContentExtractor(), new SimpleLinkExtractor(), new ParserPolicy(), true);
        } else if (options.isEnableMiner()) {
            parser = new SimpleParser(new HtmlContentExtractor(), new SimpleLinkExtractor(), new ParserPolicy(), true);
        } else {
            parser = new SimpleParser();
        }

        parser.setExtractLanguage(false);
        ParsePipe parsePipe = new ParsePipe(contentPipe, parser);

        Tap writableSeqFileSink = null;
        Pipe writableSeqFileDataPipe = null;

        // Create the output map that connects each tail pipe to the appropriate sink, and the
        // list of tail pipes.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        List<Pipe> tailPipes = new ArrayList<Pipe>();

        if (options.isGenerateHTML()) {
            // Let's write out the parse as text:
            Pipe textParsePipe = new Pipe("text parse data", parsePipe.getTailPipe());
            textParsePipe = new Each(textParsePipe, new Fields(ParsedDatum.PARSED_TEXT_FN), new RegexReplace(new Fields(ParsedDatum.PARSED_TEXT_FN), "[\\r\\n\\t]+", " ", true), Fields.REPLACE);
            textParsePipe = new Each(textParsePipe, new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN), new Identity());
            Path textParsePath = new Path(curWorkingDirPath, CrawlConfig.HTML_SUBDIR_NAME);
            Tap textParseTap = new Hfs(new TextLine(), textParsePath.toString(), true);
            sinkMap.put(textParsePipe.getName(), textParseTap);
            tailPipes.add(textParsePipe);
        }

        if (options.isEnableMiner()) {
            Pipe analyzerPipe = new Pipe("analyzer pipe", parsePipe.getTailPipe());
            analyzerPipe = new Each(analyzerPipe, analyzer);

            Pipe resultsPipe = new Pipe("results pipe", analyzerPipe);
            resultsPipe = new Each(resultsPipe, new CreateResultsFunction());

            Path minerOutputPath = new Path(curWorkingDirPath, CrawlConfig.MINER_SUBDIR_NAME);
            Tap minerOutputTap = new Hfs(new TextLine(), minerOutputPath.toString(), true);
            sinkMap.put(resultsPipe.getName(), minerOutputTap);
            tailPipes.add(resultsPipe);
        }

        // Let's output a WritableSequenceFile as an example - this file can
        // then be used as input when working with Mahout.
        writableSeqFileDataPipe = new Pipe("writable seqfile data", new Each(parsePipe.getTailPipe(), new CreateWritableSeqFileData()));

        Path writableSeqFileDataPath = new Path(curWorkingDirPath, CrawlConfig.EXTRACTED_TEXT_SUBDIR_NAME);
        writableSeqFileSink = new Hfs(new WritableSequenceFile(new Fields(CrawlConfig.WRITABLE_SEQ_FILE_KEY_FN, CrawlConfig.WRITABLE_SEQ_FILE_VALUE_FN), Text.class, Text.class),
                        writableSeqFileDataPath.toString());
        
        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlDatumFromOutlinksFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator()));
        if (urlFilter != null) {
            urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        }
        
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer()));
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

        sinkMap.put(writableSeqFileDataPipe.getName(), writableSeqFileSink);
        tailPipes.add(writableSeqFileDataPipe);
        
        FlowConnector flowConnector = new FlowConnector(props);
        Flow flow = flowConnector.connect(inputSource, sinkMap, tailPipes.toArray(new Pipe[tailPipes.size()]));

        return flow;
    }

}
