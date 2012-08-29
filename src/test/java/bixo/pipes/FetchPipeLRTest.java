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
package bixo.pipes;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.jetty.HttpException;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Response;
import org.mortbay.jetty.handler.AbstractHandler;

import bixo.config.BaseFetchJobPolicy;
import bixo.config.DefaultFetchJobPolicy;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.RedirectMode;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.AbortedFetchException;
import bixo.exceptions.AbortedFetchReason;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.IOFetchException;
import bixo.exceptions.RedirectFetchException;
import bixo.exceptions.UrlFetchException;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.RandomResponseHandler;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.fetcher.simulation.TestWebServer;
import bixo.operations.BaseGroupGenerator;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.robots.BaseRobotsParser;
import bixo.robots.SimpleRobotRulesParser;
import bixo.utils.ConfigUtils;
import bixo.utils.GroupingKey;
import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.bixolabs.cascading.Payload;

// Long-running test
@SuppressWarnings("deprecation")
public class FetchPipeLRTest extends CascadingTestCase {
    
    private static final String DEFAULT_INPUT_PATH = "build/test/FetchPipeLRTest/in";
    private static final String DEFAULT_OUTPUT_PATH = "build/test/FetchPipeLRTest/out";

    private class RedirectResponseHandler extends AbstractHandler {
        
        public static final String REDIRECT_TARGET_URL = "http://localhost:8089/redirect";
        private boolean _permanent;
        
        public RedirectResponseHandler(boolean permanent) {
            super();
            _permanent = permanent;
        }
        
        @Override
        public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
            if (_permanent) {
                // Can't use sendRedirect, as that forces it to be a temp redirect.
                if (response instanceof  Response) {
                    Response jettyResponse = (Response) response;
                    jettyResponse.setStatus(HttpStatus.SC_MOVED_PERMANENTLY);
                    jettyResponse.setHeader("Location", REDIRECT_TARGET_URL);
                }
                if (request instanceof Request) {
                    Request jettyRequest = (Request) request;
                    jettyRequest.setHandled(true);
                }
            } else {
                response.sendRedirect(REDIRECT_TARGET_URL);
            }
        }
    }

    @Test
    public void testHeadersInStatus() throws Exception {
        Lfs in = makeInputData(1, 1);

        Pipe pipe = new Pipe("urlSource");
        BaseFetcher fetcher = new FakeHttpFetcher(false, 1);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        BaseFetchJobPolicy fetchJobPolicy = new DefaultFetchJobPolicy();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, parser, fetchJobPolicy, 1);
        
        String outputPath = DEFAULT_OUTPUT_PATH;
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath, true);
        
        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, null), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath);
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        Assert.assertTrue(tupleEntryIterator.hasNext());
        StatusDatum sd = new StatusDatum(tupleEntryIterator.next());
        Assert.assertEquals(UrlStatus.FETCHED, sd.getStatus());
        HttpHeaders headers = sd.getHeaders();
        Assert.assertNotNull(headers);
        Assert.assertTrue(headers.getNames().size() > 0);
    }
    
    @Test
    public void testFetchPipe() throws Exception {
        // System.setProperty("bixo.root.level", "TRACE");
        
        final int numPages = 10;
        final int port = 8089;
        
        Lfs in = makeInputData("localhost:" + port, numPages, new Payload());

        Pipe pipe = new Pipe("urlSource");
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        BaseFetcher fetcher = new SimpleHttpFetcher(ConfigUtils.BIXO_TEST_AGENT);
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);
        
        String outputPath = "build/test/FetchPipeTest/testFetchPipe";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Tap content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        TestWebServer webServer = null;
        
        try {
            webServer = new TestWebServer(new NoRobotsResponseHandler(), port);
            flow.complete();
        } finally {
            webServer.stop();
        }
        
        // Verify numPages fetched and numPages status entries were saved.
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        
        int totalEntries = 0;
        boolean[] fetchedPages = new boolean[numPages];
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            FetchedDatum datum = new FetchedDatum(entry);
            String url = datum.getUrl();
            Assert.assertNotNull(url);
            
            // Verify that we got one of each page
            int idOffset = url.indexOf(".html") - 1;
            int pageId = Integer.parseInt(url.substring(idOffset, idOffset + 1));
            Assert.assertFalse(fetchedPages[pageId]);
            fetchedPages[pageId] = true;
        }
        
        Assert.assertEquals(numPages, totalEntries);
        tupleEntryIterator.close();
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        totalEntries = 0;
        fetchedPages = new boolean[numPages];
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            StatusDatum sd = new StatusDatum(entry);
            Assert.assertEquals(UrlStatus.FETCHED, sd.getStatus());
            
            
            // Verify that we got one of each page
            String url = sd.getUrl();
            Assert.assertNotNull(url);
            int idOffset = url.indexOf(".html") - 1;
            int pageId = Integer.parseInt(url.substring(idOffset, idOffset + 1));
            Assert.assertFalse(fetchedPages[pageId]);
            fetchedPages[pageId] = true;
        }
        
        Assert.assertEquals(numPages, totalEntries);
    }
    
    @Test
    public void testRedirectException() throws Exception {
        // System.setProperty("bixo.root.level", "TRACE");
        
        final int numPages = 1;
        final int port = 8089;
        
        Payload payload = new Payload();
        payload.put("payload-field-1", 1);
        Lfs in = makeInputData("localhost:" + port, numPages, payload);

        Pipe pipe = new Pipe("urlSource");
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        FetcherPolicy policy = new FetcherPolicy();
        policy.setRedirectMode(RedirectMode.FOLLOW_TEMP);
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);
        
        String outputPath = "build/test/FetchPipeTest/testRedirectException";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Tap content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        TestWebServer webServer = null;
        
        try {
            webServer = new TestWebServer(new RedirectResponseHandler(true), port);
            flow.complete();
        } finally {
            webServer.stop();
        }
        
        // Verify numPages fetched and numPages status entries were saved.
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        Assert.assertFalse(tupleEntryIterator.hasNext());
        tupleEntryIterator.close();
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        int totalEntries = 0;
        boolean[] fetchedPages = new boolean[numPages];
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            StatusDatum sd = new StatusDatum(entry);
            Assert.assertTrue(sd.getException() instanceof RedirectFetchException);
            RedirectFetchException redirectException =
                (RedirectFetchException)(sd.getException());
            Assert.assertEquals(RedirectResponseHandler.REDIRECT_TARGET_URL,
                                redirectException.getRedirectedUrl());
            Assert.assertEquals(payload.get("payload-field-1"),
                                sd.getPayloadValue("payload-field-1"));
            
            // Verify that we got one of each page
            String url = sd.getUrl();
            Assert.assertNotNull(url);
            int idOffset = url.indexOf(".html") - 1;
            int pageId = Integer.parseInt(url.substring(idOffset, idOffset + 1));
            Assert.assertFalse(fetchedPages[pageId]);
            fetchedPages[pageId] = true;
        }
        
        Assert.assertEquals(numPages, totalEntries);
    }
    
    @Test
    public void testFetchTerminationPipe() throws Exception {
        // System.setProperty("bixo.root.level", "TRACE");
        
        final int numPages = 10;
        final int port = 8089;
        
        Lfs in = makeInputData("localhost:" + port, numPages, null);

        Pipe pipe = new Pipe("urlSource");
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlEndTime(System.currentTimeMillis() + 20000);
        // Assume we should only need 10ms for fetching all 10 URLs.
        policy.setRequestTimeout(10);
        
        BaseFetcher fetcher = new SimpleHttpFetcher(1, policy, ConfigUtils.BIXO_TEST_AGENT);
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, 1);
        
        String outputPath = "build/test/FetchPipeTest/testFetchTerminationPipe";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, status, fetchPipe.getStatusTailPipe());
        TestWebServer webServer = null;
        
        try {
            final int numBytes = 10000;
            
            // Pick a time way longer than the FetcherPolicy.getRequestTimeout().
            final long numMilliseconds = 100 * 1000L;
            webServer = new TestWebServer(new NoRobotsResponseHandler(numBytes, numMilliseconds), port);
            flow.complete();
        } finally {
            webServer.stop();
        }
        
        Lfs validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        int totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            StatusDatum sd = new StatusDatum(entry);
            Assert.assertEquals(UrlStatus.SKIPPED_INTERRUPTED, sd.getStatus());
        }
        

        // TODO CSc - re-enable this test, when termination really works.
        // Assert.assertEquals(numPages, totalEntries);
    }
    
    @Test
    public void testPayloads() throws Exception {
        Payload payload = new Payload();
        payload.put("key", "value");
        Lfs in = makeInputData(1, 1, payload);

        Pipe pipe = new Pipe("urlSource");
        BaseFetcher fetcher = new FakeHttpFetcher(false, 10);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        BaseFetchJobPolicy fetchJobPolicy = new DefaultFetchJobPolicy();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, parser, fetchJobPolicy, 1);
        
        String outputPath = "build/test/FetchPipeTest/dual";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Tap content = new Hfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        
        int totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;
            
            FetchedDatum datum = new FetchedDatum(entry);
            String payloadValue = (String)datum.getPayloadValue("key");
            Assert.assertNotNull(payloadValue);
            Assert.assertEquals("value", payloadValue);
        }
        
        Assert.assertEquals(1, totalEntries);
        tupleEntryIterator.close();
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new JobConf());
        totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            StatusDatum sd = new StatusDatum(entry);
            Assert.assertEquals(UrlStatus.FETCHED, sd.getStatus());
            String payloadValue = (String)sd.getPayloadValue("key");
            Assert.assertNotNull(payloadValue);
            Assert.assertEquals("value", payloadValue);
        }
        
        Assert.assertEquals(1, totalEntries);
    }
    
    @Test
    public void testSkippingURLsByScore() throws Exception {
        Lfs in = makeInputData(1, 1);

        Pipe pipe = new Pipe("urlSource");
        BaseFetcher fetcher = new FakeHttpFetcher(false, 1);
        BaseScoreGenerator scorer = new SkippedScoreGenerator();
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        BaseFetchJobPolicy fetchJobPolicy = new DefaultFetchJobPolicy();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, parser, fetchJobPolicy, 1);
        
        String outputPath = DEFAULT_OUTPUT_PATH;
        Lfs content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);
        
        // Finally we can run it.
        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(null, content), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        Assert.assertFalse(tupleEntryIterator.hasNext());
    }
    
    @Test
    public void testDurationLimitSimple() throws Exception {
        // Pretend like we have 10 URLs from the same domain
        Lfs in = makeInputData(1, 10);

        // Create the fetch pipe we'll use to process these fake URLs
        Pipe pipe = new Pipe("urlSource");
        
        // This will force all URLs to get skipped because of the crawl end time limit.
        FetcherPolicy defaultPolicy = new FetcherPolicy();
        defaultPolicy.setCrawlEndTime(0);
        BaseFetcher fetcher = new FakeHttpFetcher(false, 1, defaultPolicy);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        BaseFetchJobPolicy fetchJobPolicy = new DefaultFetchJobPolicy(defaultPolicy);
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, parser, fetchJobPolicy, 1);

        // Create the output
        String outputPath = DEFAULT_OUTPUT_PATH;
        Tap statusSink = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Tap contentSink = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(statusSink, contentSink), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        Assert.assertFalse(tupleEntryIterator.hasNext());
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        
        int numEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            numEntries += 1;
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum status = new StatusDatum(entry);
            Assert.assertEquals(UrlStatus.SKIPPED_TIME_LIMIT, status.getStatus());
        }
        
        Assert.assertEquals(10, numEntries);
    }
    
    @Test
    public void testMaxUrlsPerServer() throws Exception {
        // Pretend like we have 2 URLs from the same domain
        final int sourceUrls = 2;
        Lfs in = makeInputData(1, sourceUrls);

        // Create the fetch pipe we'll use to process these fake URLs
        Pipe pipe = new Pipe("urlSource");
        
        // This will limit us to one URL.
        final int maxUrls = 1;
        FetcherPolicy defaultPolicy = new FetcherPolicy();
        BaseFetcher fetcher = new FakeHttpFetcher(false, 1, defaultPolicy);
        BaseScoreGenerator scorer = new FixedScoreGenerator();
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        BaseFetchJobPolicy fetchJobPolicy = new DefaultFetchJobPolicy(defaultPolicy.getMaxRequestsPerConnection(), maxUrls, BaseFetchJobPolicy.DEFAULT_CRAWL_DELAY);
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, parser, fetchJobPolicy, 1);

        // Create the output
        String outputPath = DEFAULT_OUTPUT_PATH;
        Lfs statusSink = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Lfs contentSink = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(statusSink, contentSink), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        Assert.assertTrue(tupleEntryIterator.hasNext());
        tupleEntryIterator.next();
        Assert.assertFalse(tupleEntryIterator.hasNext());

        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new HadoopFlowProcess());
        
        int numSkippedEntries = 0;
        int numFetchedEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum status = new StatusDatum(entry);
            if (status.getStatus() == UrlStatus.SKIPPED_PER_SERVER_LIMIT) {
                numSkippedEntries += 1;
            } else if (status.getStatus() == UrlStatus.FETCHED) {
                numFetchedEntries += 1;
            } else {
                Assert.fail("Unexpected status: " + status.getStatus());
            }
        }
        
        Assert.assertEquals(numFetchedEntries, maxUrls);
        Assert.assertEquals(numSkippedEntries, sourceUrls - maxUrls);
    }
    
    // TODO KKr- re-enable this test when we know how to make it work for
    // the new fetcher architecture.
    /**
    @Test
    public void testPassingAllStatus() throws Exception {
        // Pretend like we have 10 URLs from one domain, to match the
        // 10 cases we need to test.
        Lfs in = makeInputData(1, 10);

        // Create the fetch pipe we'll use to process these fake URLs
        Pipe pipe = new Pipe("urlSource");
        
        // We need to skip things for all the SKIPPED/ABORTED/ERROR reasons in
        // UrlStatus, plus one of the HTTP reasons. Note that we don't do
        // SKIPPED_TIME_LIMIT, since that's hard to test in the middle of testing
        // everything else.
//        SKIPPED_BLOCKED,            // Blocked by robots.txt
//        SKIPPED_UNKNOWN_HOST,       // Hostname couldn't be resolved to IP address
//        SKIPPED_INVALID_URL,        // URL invalid
//        SKIPPED_DEFERRED,           // Deferred because robots.txt couldn't be processed.
//        SKIPPED_BY_SCORER,          // Skipped explicitly by scorer
//        SKIPPED_BY_SCORE,           // Skipped because score wasn't high enough
//        ABORTED_SLOW_RESPONSE,
//        ABORTED_INVALID_MIMETYPE
//        HTTP_NOT_FOUND,
//        ERROR_INVALID_URL,
//        ERROR_IOEXCEPTION,

        FetchPipe fetchPipe = new FetchPipe(pipe, new CustomGrouper(), new CustomScorer(), new CustomFetcher());

        // Create the output
        String outputPath = DEFAULT_OUTPUT_PATH;
        Tap statusSink = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Tap contentSink = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(statusSink, contentSink), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        Assert.assertFalse(tupleEntryIterator.hasNext());
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new JobConf());
        
        int numStatus = UrlStatus.values().length;
        boolean returnedStatus[] = new boolean[numStatus];
        
        Fields metaDataFields = new Fields();
        int numEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            numEntries += 1;
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum status = new StatusDatum(entry);
            int ordinal = status.getStatus().ordinal();
            Assert.assertFalse(returnedStatus[ordinal]);
            returnedStatus[ordinal] = true;
        }
        
        Assert.assertEquals(10, numEntries);
    }
    */
    
    @SuppressWarnings("serial")
    private static class SkippedScoreGenerator extends BaseScoreGenerator {

        @Override
        public double generateScore(String domain, String pld, String url) {
            return BaseScoreGenerator.SKIP_SCORE;
        }
    }
    
    /**
    @SuppressWarnings("serial")
    private static class RandomScoreGenerator implements IScoreGenerator {

        private double _minScore;
        private double _maxScore;
        private Random _rand;
        
        public RandomScoreGenerator(double minScore, double maxScore) {
            _minScore = minScore;
            _maxScore = maxScore;
            _rand = new Random();
        }
        
        @Override
        public double generateScore(GroupedUrlDatum urlTuple) {
            double range = _maxScore - _minScore;
            
            return _minScore + (_rand.nextDouble() * range);
        }
    }
    **/
    
    private Lfs makeInputData(int numDomains, int numPages) throws IOException {
        return makeInputData(numDomains, numPages, null);
    }
    
    private Lfs makeInputData(int numDomains, int numPages, Payload payload) throws IOException {
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), DEFAULT_INPUT_PATH, true);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        for (int i = 0; i < numDomains; i++) {
            for (int j = 0; j < numPages; j++) {
                // Use special domain name pattern so code deep inside of operations "knows" not
                // to try to resolve host names to IP addresses.
                write.add(makeTuple("bixo-test-domain-" + i + ".com", j, payload));
            }
        }
        
        write.close();
        return in;
    }
    
    private Lfs makeInputData(String domain, int numPages, Payload payload) throws IOException {
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), DEFAULT_INPUT_PATH, true);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        for (int j = 0; j < numPages; j++) {
            write.add(makeTuple(domain, j, payload));
        }

        write.close();
        return in;
    }
    
    private Tuple makeTuple(String domain, int pageNumber, Payload payload) {
        UrlDatum url = new UrlDatum("http://" + domain + "/page-" + pageNumber + ".html?size=10");
        url.setPayload(payload);
        return url.getTuple();
    }
    
    private static class NoRobotsResponseHandler extends RandomResponseHandler {

        public NoRobotsResponseHandler() {
            super(1000, 10);
        }
        
        public NoRobotsResponseHandler(int length, long duration) {
            super(length, duration);
        }
        
        @Override
        public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
            if (pathInContext.endsWith("/robots.txt")) {
                throw new HttpException(HttpStatus.SC_NOT_FOUND, "No robots.txt");
            } else {
                super.handle(pathInContext, request, response, dispatch);
            }
        }
    }
    
    @SuppressWarnings("unused")
    private static class NoRobotsHtmlResponseHandler extends AbstractHandler {

        @Override
        public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
            if (pathInContext.endsWith("/robots.txt")) {
                throw new HttpException(HttpStatus.SC_NOT_FOUND, "No robots.txt");
            } else {
                final String template = "<htm><head><title>%s</title></head><body></body></html>";
                
                String htmlResponse = String.format(template, pathInContext.substring(1));
                byte[] content = htmlResponse.getBytes("UTF-8");
                response.setContentLength(content.length);
                response.setContentType("text/html; charset=UTF-8");
                response.setStatus(200);
                
                OutputStream os = response.getOutputStream();
                os.write(content);
                os.flush();
            }
        }
    }
    
    /***********************************************************************
     * Lots of ugly custom classes to support serializable "mocking" for a
     * particular test case. Mockito mocks aren't serializable,
     * or at least I couldn't see an easy way to make this work.
     */

    @SuppressWarnings({ "serial", "unused" })
    private static class CustomGrouper extends BaseGroupGenerator {

        @Override
        public String getGroupingKey(UrlDatum urlDatum) {
            String url = urlDatum.getUrl();
            if (url.contains("page-0")) {
                return GroupingKey.BLOCKED_GROUPING_KEY;
            } else if (url.contains("page-1")) {
                return GroupingKey.UNKNOWN_HOST_GROUPING_KEY;
            } else if (url.contains("page-2")) {
                return GroupingKey.INVALID_URL_GROUPING_KEY;
            } else if (url.contains("page-3")) {
                return GroupingKey.DEFERRED_GROUPING_KEY;
            } else if (url.contains("page-4")) {
                return GroupingKey.SKIPPED_GROUPING_KEY;
            } else {
                return GroupingKey.makeGroupingKey("domain-0.com", 30000);
            }
        }
    };
    
    /**
    @SuppressWarnings("serial")
    private static class CustomScorer implements IScoreGenerator {

        @Override
        public double generateScore(GroupedUrlDatum urlDatum) {
            String url = urlDatum.getUrl();
            if (url.contains("page-5")) {
                return 0.0;
            } else {
                return 10.0;
            }
        }
    };
    **/
    
    @SuppressWarnings("serial")
    private static class MaxUrlFetcherPolicy extends FetcherPolicy {
        private int _maxUrls;
        
        public MaxUrlFetcherPolicy(int maxUrls) {
            super();

            _maxUrls = maxUrls;
        }
        
        @Override
        public int getMaxUrls() {
            return _maxUrls;
        }
    }
    
    @SuppressWarnings({ "serial", "unused" })
    private static class CustomFetcher extends BaseFetcher {

        public CustomFetcher() {
            super(1, new MaxUrlFetcherPolicy(4), ConfigUtils.BIXO_TEST_AGENT);
        }
        
        @Override
        public FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException {
            String url = scoredUrl.getUrl();
            if (url.contains("page-6")) {
                throw new AbortedFetchException(url, AbortedFetchReason.SLOW_RESPONSE_RATE);
            } else if (url.contains("page-7")) {
                throw new HttpFetchException(url, "msg", HttpStatus.SC_GONE, new HttpHeaders());
            }  else if (url.contains("page-8")) {
                throw new IOFetchException(url, new IOException());
            } else if (url.contains("page-9")) {
                throw new UrlFetchException(url, "msg");
            } else {
                throw new RuntimeException("Unexpected page");
            }
        }

	    @Override
	    public void abort() {
	        // Do nothing
	    }

    };


    

}
