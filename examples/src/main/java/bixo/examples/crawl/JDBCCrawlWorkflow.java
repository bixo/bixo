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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.operations.NormalizeUrlFunction;
import bixo.operations.UrlFilter;
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
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.NullContext;

public class JDBCCrawlWorkflow {
    /*
    private static final Logger LOGGER = Logger.getLogger(JDBCCrawlWorkflow.class);

    @SuppressWarnings({"serial", "rawtypes"})
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
            
            if (bestDatum != null && CrawlConfig.isUnfetchedStatus(bestDatum.getLastStatus())) {
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

    
    @SuppressWarnings("rawtypes")
    public static Flow createFlow(BixoPlatform platform, BasePath inputDir, BasePath curLoopDirPath, UserAgent userAgent, FetcherPolicy fetcherPolicy,
                    BaseUrlFilter urlFilter, int maxThreads, boolean debug, String persistentDbLocation) throws Throwable {

        platform.resetNumReduceTasks();

        if (!inputDir.exists()) {
            throw new IllegalStateException(String.format("Input directory %s doesn't exist", inputDir));
        }

        Tap inputSource = JDBCTapFactory.createUrlsSourceJDBCTap(platform, persistentDbLocation);

        // Read _everything_ in initially
        // Group on the url, and select the best urls to best
        Pipe importPipe = new Pipe("url importer");
        importPipe = new GroupBy(importPipe, new Fields(CrawlDbDatum.URL_FIELD));
        importPipe = new Every(importPipe, new BestUrlToFetchBuffer(), Fields.RESULTS);

        BasePath contentPath = platform.makePath(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
        Tap contentSink = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), contentPath);

        BasePath parsePath = platform.makePath(curLoopDirPath, CrawlConfig.PARSE_SUBDIR_NAME);
        Tap parseSink = platform.makeTap(platform.makeBinaryScheme(ParsedDatum.FIELDS), parsePath);
        
        BasePath statusDirPath = platform.makePath(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusSink = platform.makeTap(platform.makeTextScheme(), statusDirPath);

        // NOTE: The source and sink for CrawlDbDatums is essentially the same database -
        // since cascading doesn't allow you to use the same tap for source and 
        // sink we fake it by creating two separate taps.
        Tap urlSink = JDBCTapFactory.createUrlsSinkJDBCTap(platform, persistentDbLocation);

        // Create the sub-assembly that runs the fetch job
        BaseFetcher fetcher = new SimpleHttpFetcher(maxThreads, fetcherPolicy, userAgent);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(importPipe, scorer, fetcher, platform.getNumReduceTasks());

        Pipe statusPipe = new Pipe("status pipe", fetchPipe.getStatusTailPipe());

        // Take content and split it into content output plus parse to extract URLs.
        ParsePipe parsePipe = new ParsePipe(fetchPipe.getContentTailPipe(), new SimpleParser());
        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", parsePipe.getTailPipe());
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new CreateUrlDatumFromOutlinksFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator()));
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new NormalizeUrlFunction(new SimpleUrlNormalizer()));

        // Take status and output updated UrlDatum's. Again, since we are using
        // the same database we need to create a new tap.
        Pipe urlFromFetchPipe = new Pipe("url from fetch", fetchPipe.getStatusTailPipe());
        urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlDatumFromStatusFunction());

        // Now we need to join the URLs we get from parsing content with the
        // URLs we got from the status output, so we have a unified stream
        // of all known URLs.
        Pipe urlPipe = new GroupBy("url pipe", Pipe.pipes(urlFromFetchPipe, urlFromOutlinksPipe), new Fields(UrlDatum.URL_FN));
        urlPipe = new Every(urlPipe, new LatestUrlDatumBuffer(), Fields.RESULTS);

        Pipe outputPipe = new Pipe ("output pipe");
        outputPipe = new Each(urlPipe, new CreateCrawlDbDatumFromUrlFunction());

        // Create the output map that connects each tail pipe to the appropriate sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(statusPipe.getName(), statusSink);
        sinkMap.put(FetchPipe.CONTENT_PIPE_NAME, contentSink);
        sinkMap.put(ParsePipe.PARSE_PIPE_NAME, parseSink);
        sinkMap.put(outputPipe.getName(), urlSink);
        
        // Finally we can run it.
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(inputSource, sinkMap, statusPipe, fetchPipe.getContentTailPipe(), parsePipe.getTailPipe(), outputPipe);
            
    }
*/
}
