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
package bixo.fetcher;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.BaseFetchException;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.operations.LoadUrlsFunction;
import bixo.pipes.FetchPipe;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;

@SuppressWarnings("deprecation")
public class FetcherTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetcherTest.class);
    
    private static final String URL_DB_NAME = "url_db";

    @SuppressWarnings("serial")
    private static class FirefoxUserAgent extends UserAgent {
        public FirefoxUserAgent() {
            super("Firefox", "", "");
        }
        
        @Override
        public String getUserAgentString() {
            // Use standard Firefox agent name, as some sites won't work w/non-standard names.
            return "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.8) Gecko/2009032608 Firefox/3.0.8";
        }
    }
        
    @SuppressWarnings("rawtypes")
    private String makeCrawlDb(String workingFolder, String input) throws Exception {

        BixoPlatform platform = new BixoPlatform(FetcherTest.class, Platform.Local);
        
        // We don't want to regenerate this DB all the time.
        BasePath workingPath = platform.makePath(workingFolder);
        BasePath crawlDBPath = platform.makePath(workingPath, URL_DB_NAME);
        if (!crawlDBPath.exists()) {
            Pipe importPipe = new Pipe("import URLs");
            importPipe = new Each(importPipe, new LoadUrlsFunction());
            
            BasePath inputPath = platform.makePath(input);
            Tap sourceTap = platform.makeTap(platform.makeTextScheme(), inputPath);
            Tap sinkTap = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), crawlDBPath, SinkMode.REPLACE);
            
            FlowConnector flowConnector = platform.makeFlowConnector();
            Flow flow = flowConnector.connect(sourceTap, sinkTap, importPipe);
            flow.complete();
        }

        return crawlDBPath.getAbsolutePath();
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testStaleConnection() throws Exception {
        System.setProperty("bixo.root.level", "TRACE");

        String workingFolder = "build/it/FetcherTest/testStaleConnection/working";
        String input = makeCrawlDb(workingFolder, "src/it/resources/apple-pages.txt");
        
        BixoPlatform platform = new BixoPlatform(FetcherTest.class, Platform.Local);
        BasePath inputPath = platform.makePath(input);

        Tap in = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), inputPath);
        String outputDir = "build/it/FetcherTest/testStaleConnection/out";
        BasePath outputPath = platform.makePath(outputDir);

        BasePath contentPath = platform.makePath(outputPath, "content");
        Tap content = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), contentPath, SinkMode.REPLACE);

        BasePath statusPath = platform.makePath(outputPath, "status");
        Tap status = platform.makeTap(platform.makeBinaryScheme(StatusDatum.FIELDS), statusPath, SinkMode.REPLACE);

        
        Pipe pipe = new Pipe("urlSource");

        UserAgent userAgent = new FirefoxUserAgent();
        FetcherPolicy fetcherPolicy = new FetcherPolicy();
        fetcherPolicy.setMaxRequestsPerConnection(1);
        fetcherPolicy.setCrawlDelay(5 * 1000L);
        BaseFetcher fetcher = new SimpleHttpFetcher(2, fetcherPolicy, userAgent);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);

        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        // Test for all valid fetches.
        Tap validate = platform.makeTap(platform.makeBinaryScheme(StatusDatum.FIELDS), statusPath);
        TupleEntryIterator tupleEntryIterator = validate.openForRead(platform.makeFlowProcess());
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum sd = new StatusDatum(entry);
            if (sd.getStatus() != UrlStatus.FETCHED) {
                LOGGER.error(String.format("Fetched failed! Status is %s for %s", sd.getStatus(), sd.getUrl()));
                BaseFetchException e = sd.getException();
                if (e != null) {
                    LOGGER.error("Fetched failed due to exception", e);
                }
                
                Assert.fail("Status not equal to FETCHED");
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testRunFetcher() throws Exception {
        System.setProperty("bixo.root.level", "TRACE");
        
        String workingFolder = "build/test-it/FetcherTest/testRunFetcher";
        String input = makeCrawlDb(workingFolder, "src/it/resources/top10urls.txt");
        BixoPlatform platform = new BixoPlatform(FetcherTest.class, Platform.Local);
        BasePath workingPath = platform.makePath(workingFolder);
        BasePath inputPath = platform.makePath(input);
        Tap in = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), inputPath);
        
        BasePath contentPath = platform.makePath(workingPath, "content");
        Tap content = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), contentPath, SinkMode.REPLACE);
        BasePath statusPath = platform.makePath(workingPath, "status");
        Tap status = platform.makeTap(platform.makeTextScheme(), statusPath, SinkMode.REPLACE);

        
        Pipe pipe = new Pipe("urlSource");

        UserAgent userAgent = new FirefoxUserAgent();
        BaseFetcher fetcher = new SimpleHttpFetcher(10, userAgent);
        // Microsoft has a really big home page.
        fetcher.setDefaultMaxContentSize(256 * 1024);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);

        FlowConnector flowConnector = platform.makeFlowConnector();

        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        // Test for 10 good fetches.
        Tap validate = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), contentPath);
        TupleEntryIterator tupleEntryIterator = validate.openForRead(platform.makeFlowProcess());
        int fetchedPages = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            new FetchedDatum(entry);
            fetchedPages += 1;
        }

        Assert.assertEquals(10, fetchedPages);
    }
    
}
