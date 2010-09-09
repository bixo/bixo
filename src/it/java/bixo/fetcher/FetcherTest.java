/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.fetcher;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.Test;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.FetchException;
import bixo.fetcher.http.HttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.FixedScoreGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.operations.LoadUrlsFunction;
import bixo.pipes.FetchPipe;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class FetcherTest {
    private static final Logger LOGGER = Logger.getLogger(FetcherTest.class);
    
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
        
    private String makeCrawlDb(String workingFolder, String inputPath) throws IOException {

        // We don't want to regenerate this DB all the time.
        File crawlDBFile = new File(workingFolder, URL_DB_NAME);
        String crawlDBPath = crawlDBFile.getAbsolutePath();
        if (!crawlDBFile.exists()) {
            Pipe importPipe = new Pipe("import URLs");
            importPipe = new Each(importPipe, new LoadUrlsFunction());
            
            Tap sourceTap = new Lfs(new TextLine(), inputPath);
            Tap sinkTap = new Lfs(new SequenceFile(UrlDatum.FIELDS), crawlDBPath, true);
            
            FlowConnector flowConnector = new FlowConnector();
            Flow flow = flowConnector.connect(sourceTap, sinkTap, importPipe);
            flow.complete();
        }

        return crawlDBPath;
    }
    
    @Test
    public void testStaleConnection() throws Exception {
        System.setProperty("bixo.root.level", "TRACE");

        String workingFolder = "build/it/FetcherTest/testStaleConnection/working";
        String inputPath = makeCrawlDb(workingFolder, "src/it/resources/apple-pages.txt");
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), inputPath, true);
        String outPath = "build/it/FetcherTest/testStaleConnection/out";
        Lfs content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outPath + "/content", true);
        Lfs status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outPath + "/status", true);
        
        Pipe pipe = new Pipe("urlSource");

        UserAgent userAgent = new FirefoxUserAgent();
        FetcherPolicy fetcherPolicy = new FetcherPolicy();
        fetcherPolicy.setMaxRequestsPerConnection(1);
        fetcherPolicy.setCrawlDelay(5 * 1000L);
        HttpFetcher fetcher = new SimpleHttpFetcher(2, fetcherPolicy, userAgent);
        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);

        FlowConnector flowConnector = new FlowConnector();

        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        // Test for all valid fetches.
        Lfs validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outPath + "/status");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum sd = new StatusDatum(entry);
            if (sd.getStatus() != UrlStatus.FETCHED) {
                LOGGER.error(String.format("Fetched failed! Status is %s for %s", sd.getStatus(), sd.getUrl()));
                FetchException e = sd.getException();
                if (e != null) {
                    LOGGER.error("Fetched failed due to exception", e);
                }
                
                Assert.fail("Status not equal to FETCHED");
            }
        }
    }

    @Test
    public void testRunFetcher() throws Exception {
        System.setProperty("bixo.root.level", "TRACE");
        
        String workingFolder = "build/test-it/FetcherTest/testRunFetcher";
        String inputPath = makeCrawlDb(workingFolder, "src/it/resources/top10urls.txt");
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), inputPath, true);
        Lfs content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), workingFolder + "/content", true);
        Lfs status = new Lfs(new TextLine(), workingFolder + "/status", true);

        Pipe pipe = new Pipe("urlSource");

        UserAgent userAgent = new FirefoxUserAgent();
        HttpFetcher fetcher = new SimpleHttpFetcher(10, userAgent);
        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);

        FlowConnector flowConnector = new FlowConnector();

        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        // Test for 10 good fetches.
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), workingFolder + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        int fetchedPages = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            new FetchedDatum(entry);
            fetchedPages += 1;
        }

        Assert.assertEquals(10, fetchedPages);
    }
    
}
