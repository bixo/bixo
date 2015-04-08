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
package bixo.examples.webmining;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.ParserPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.parser.SimpleParser;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParsePipe;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.IoUtils;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.filter.Limit;
import cascading.operation.filter.Limit.Context;
import cascading.operation.regex.RegexParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.OuterJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.BaseSplitter;
import com.scaleunlimited.cascading.SplitterAssembly;
import com.scaleunlimited.cascading.TupleLogger;

public class DemoWebMiningWorkflow {

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
    
    @SuppressWarnings({"serial", "rawtypes"})
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


            int numTasks = flowProcess.getNumProcessSlices();
            int taskNum = flowProcess.getCurrentSliceNum();

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


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void importSeedUrls(BasePlatform platform, BasePath crawlDbPath, String fileName) throws Exception  {
        
        SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
        
        InputStream is = null;
        TupleEntryCollector writer = null;
        try {
            Tap urlSink = platform.makeTap(platform.makeTextScheme(), crawlDbPath, SinkMode.REPLACE);
            writer = urlSink.openForWrite(platform.makeFlowProcess());

            is = DemoWebMiningWorkflow.class.getResourceAsStream(fileName);
            if (is == null) {
                throw new FileNotFoundException("The seed urls file doesn't exist");
            }

            List<String> lines = IOUtils.readLines(is);
            for (String line : lines) {
                line = line.trim();
                if (line.startsWith("#")) {
                    continue;
                }

                CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize(line), 0, UrlStatus.UNFETCHED, 0.0f, 0.0f);
                writer.add(datum.getTuple());
            }

        } catch (IOException e) {
            crawlDbPath.delete(true);
            throw e;
        } finally {
            IoUtils.safeClose(is);
            if (writer != null) {
                writer.close();
            }
        }

    }
    
    @SuppressWarnings("rawtypes")
    public static Flow createWebMiningWorkflow(BixoPlatform platform, BasePath crawlDbPath, BasePath curLoopDirPath, FetcherPolicy fetcherPolicy, UserAgent userAgent, 
                    DemoWebMiningOptions options) throws Exception {
        
        // Fetch at most 200 pages, max size of 128K, complete mode, from the current dir.
        // HTML only.
        
        // We want to extract the cleaned up HTML, and pass that to the parser, which will
        // be specified via options.getAnalyzer. From this we'll get outlinks, page score, and
        // any results.
        
        boolean isLocal = platform.isLocal();
        platform.setNumReduceTasks(options.getNumReduceTasks());
        platform.setProperty("mapred.min.split.size", 64 * 1024 * 1024);

        // Input : the crawldb
        crawlDbPath.assertExists("CrawlDb");

// TODO VMa - figure out types       Tap inputSource = platform.makeTap(new TextDelimited(CrawlDbDatum.FIELDS, "\t", CrawlDbDatum.TYPES), crawlDbPath);
        Tap inputSource = platform.makeTap(platform.makeTextScheme(), crawlDbPath);
        Pipe importPipe = new Pipe("import pipe");
        // Apply a regex to extract the relevant fields 
        RegexParser crawlDbParser = new RegexParser(CrawlDbDatum.FIELDS, 
                                                        "^(.*?)\t(.*?)\t(.*?)\t(.*?)\t(.*)");
        importPipe = new Each(importPipe, new Fields("line"), crawlDbParser);

        // Split into tuples that are to be fetched and that have already been fetched
        SplitterAssembly splitter = new SplitterAssembly(importPipe, new SplitFetchedUnfetchedSSCrawlDatums());

        Pipe finishedDatumsFromDb = new Pipe("finished datums from db", splitter.getRHSPipe());
        Pipe urlsToFetchPipe = splitter.getLHSPipe();

        // Limit to MAX_DISTRIBUTED_FETCH if running in real cluster, 
        // or MAX_LOCAL_FETCH if running locally. So first we sort the entries 
        // from high to low by links score.
        // TODO add unit test
        urlsToFetchPipe = new GroupBy(urlsToFetchPipe, new Fields(CrawlDbDatum.LINKS_SCORE_FIELD), true);
        long maxToFetch = isLocal ? MAX_LOCAL_FETCH : MAX_DISTRIBUTED_FETCH;
        urlsToFetchPipe = new Each(urlsToFetchPipe, new CreateUrlDatumFromCrawlDbDatum(maxToFetch));

        BaseScoreGenerator scorer = new LinkScoreGenerator();

        // Create the sub-assembly that runs the fetch job
        int maxThreads = isLocal ? CrawlConfig.DEFAULT_NUM_THREADS_LOCAL :  CrawlConfig.DEFAULT_NUM_THREADS_CLUSTER;
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, fetcherPolicy, userAgent);
        fetcher.setMaxRetryCount(CrawlConfig.MAX_RETRIES);
        fetcher.setSocketTimeout(CrawlConfig.SOCKET_TIMEOUT);
        fetcher.setConnectionTimeout(CrawlConfig.CONNECTION_TIMEOUT);

        FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, platform.getNumReduceTasks());
        Pipe statusPipe = new Pipe("status pipe", fetchPipe.getStatusTailPipe());
        Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());
        contentPipe = TupleLogger.makePipe(contentPipe, true);

        // Create a parser that returns back the raw HTML (cleaned up by Tika) as the parsed content.
        SimpleParser parser = new SimpleParser(new ParserPolicy(), true);
        ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), parser);
        
        Pipe analyzerPipe = new Pipe("analyzer pipe");
        analyzerPipe = new Each(parsePipe.getTailPipe(), new AnalyzeHtml());
        
        Pipe outlinksPipe = new Pipe("outlinks pipe", analyzerPipe);
        outlinksPipe = new Each(outlinksPipe, new CreateLinkDatumFromOutlinksFunction());

        Pipe resultsPipe = new Pipe("results pipe", analyzerPipe);
        resultsPipe = new Each(resultsPipe, new CreateResultsFunction());
        
        // Group the finished datums, the skipped datums, status, outlinks
        Pipe updatePipe = new CoGroup("update pipe", Pipe.pipes(finishedDatumsFromDb, statusPipe, analyzerPipe, outlinksPipe), 
                        Fields.fields(new Fields(CrawlDbDatum.URL_FIELD), new Fields(StatusDatum.URL_FN), 
                                        new Fields(AnalyzedDatum.URL_FIELD), new Fields(LinkDatum.URL_FN)), null, new OuterJoin());
        updatePipe = new Every(updatePipe, new UpdateCrawlDbBuffer(), Fields.RESULTS);

        
        // output : loop dir specific crawldb
        BasePath outCrawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap crawlDbSink = platform.makeTap(platform.makeTextScheme(), outCrawlDbPath, SinkMode.REPLACE);
        // Status, 
        BasePath statusDirPath = platform.makePath(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusSink = platform.makeTap(platform.makeTextScheme(), statusDirPath);
        // Content
        BasePath contentDirPath = platform.makePath(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
        Tap contentSink = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), contentDirPath);
        
        // PageResults
        BasePath resultsDirPath = platform.makePath(curLoopDirPath, CrawlConfig.RESULTS_SUBDIR_NAME);
        Tap resultsSink = platform.makeTap(platform.makeTextScheme(), resultsDirPath);

        // Create the output map that connects each tail pipe to the appropriate sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(updatePipe.getName(), crawlDbSink);
        sinkMap.put(statusPipe.getName(), statusSink);
        sinkMap.put(contentPipe.getName(), contentSink);
        sinkMap.put(resultsPipe.getName(), resultsSink);

        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(inputSource, sinkMap, updatePipe, statusPipe, contentPipe, resultsPipe);

        return flow;
    }

}
