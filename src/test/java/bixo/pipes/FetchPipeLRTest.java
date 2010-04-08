package bixo.pipes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.BaseDatum;
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
import bixo.exceptions.UrlFetchException;
import bixo.fetcher.RandomResponseHandler;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.fetcher.simulation.TestWebServer;
import bixo.fetcher.util.FixedScoreGenerator;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.utils.ConfigUtils;
import bixo.utils.GroupingKey;
import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

// Long-running test
public class FetchPipeLRTest extends CascadingTestCase {
    
    @Test
    public void testHeadersInStatus() throws Exception {
        Lfs in = makeInputData(1, 1);

        Pipe pipe = new Pipe("urlSource");
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 1);
        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, FetcherPolicy.NO_CRAWL_END_TIME, 1, BaseDatum.EMPTY_METADATA_FIELDS);
        
        String outputPath = "build/test/FetchPipeTest-testHeadersInStatus/out";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath, true);
        
        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, null), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath);
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        Assert.assertTrue(tupleEntryIterator.hasNext());
        StatusDatum sd = new StatusDatum(tupleEntryIterator.next(), new Fields());
        Assert.assertEquals(UrlStatus.FETCHED, sd.getStatus());
        HttpHeaders headers = sd.getHeaders();
        Assert.assertNotNull(headers);
        Assert.assertTrue(headers.getNames().size() > 0);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testFetchPipe() throws Exception {
        // System.setProperty("bixo.root.level", "TRACE");
        
        final int numPages = 10;
        final int port = 8089;
        
        Fields metaDataFields = new Fields("meta-test");
        Map<String, Comparable> metadata = new HashMap<String, Comparable>();
        metadata.put("meta-test", "value");
        Lfs in = makeInputData("localhost:" + port, numPages, metadata);

        Pipe pipe = new Pipe("urlSource");
        ScoreGenerator scorer = new FixedScoreGenerator();
        IHttpFetcher fetcher = new SimpleHttpFetcher(ConfigUtils.BIXO_TEST_AGENT);
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, FetcherPolicy.NO_CRAWL_END_TIME, 1, metaDataFields);
        
        String outputPath = "build/test/FetchPipeTest/testFetchPipe";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS.append(metaDataFields)), outputPath + "/status", true);
        Tap content = new Lfs(new SequenceFile(FetchedDatum.FIELDS.append(metaDataFields)), outputPath + "/content", true);

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
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS.append(metaDataFields)), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        
        int totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            FetchedDatum datum = new FetchedDatum(entry, metaDataFields);
            Assert.assertNotNull(datum.getBaseUrl());
            Assert.assertNotNull(datum.getFetchedUrl());
            
            // Verify metadata
            String metaValue = entry.getString("meta-test");
            Assert.assertNotNull(metaValue);
            Assert.assertEquals("value", metaValue);
        }
                
        Assert.assertEquals(numPages, totalEntries);
        tupleEntryIterator.close();
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS.append(metaDataFields)), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new JobConf());
        totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            StatusDatum sd = new StatusDatum(entry, metaDataFields);
            Assert.assertEquals(UrlStatus.FETCHED, sd.getStatus());
        }
        
        Assert.assertEquals(numPages, totalEntries);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testMetaData() throws Exception {
        Map<String, Comparable> metaData = new HashMap<String, Comparable>();
        metaData.put("key", "value");
        Lfs in = makeInputData(1, 1, metaData);

        Pipe pipe = new Pipe("urlSource");
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 10);
        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, FetcherPolicy.NO_CRAWL_END_TIME, 1, new Fields("key"));
        
        String outputPath = "build/test/FetchPipeTest/dual";
        Fields contentFields = FetchedDatum.FIELDS.append(new Fields("key"));
        Tap content = new Hfs(new SequenceFile(contentFields), outputPath + "/content", true);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(null, content), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(contentFields), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        
        int totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;
            
            Assert.assertEquals(entry.size(), contentFields.size());
            String metaValue = entry.getString("key");
            Assert.assertNotNull(metaValue);
            Assert.assertEquals("value", metaValue);
        }
        
        Assert.assertEquals(1, totalEntries);
    }
    
    @Test
    public void testSkippingURLsByScore() throws Exception {
        Lfs in = makeInputData(1, 1);

        Pipe pipe = new Pipe("urlSource");
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 1);
        ScoreGenerator scorer = new SkippedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, FetcherPolicy.NO_CRAWL_END_TIME, 1, BaseDatum.EMPTY_METADATA_FIELDS);
        
        String outputPath = "build/test/FetchPipeTest/out";
        Tap content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);
        
        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(null, content), fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
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
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 1, defaultPolicy);
        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, 0, 1, BaseDatum.EMPTY_METADATA_FIELDS);

        // Create the output
        String outputPath = "build/test/FetchPipeTest/out";
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
        
        Fields metaDataFields = new Fields();
        int numEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            numEntries += 1;
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum status = new StatusDatum(entry, metaDataFields);
            Assert.assertEquals(UrlStatus.SKIPPED_TIME_LIMIT, status.getStatus());
        }
        
        Assert.assertEquals(10, numEntries);
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
        String outputPath = "build/test/FetchPipeTest/out";
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
            StatusDatum status = new StatusDatum(entry, metaDataFields);
            int ordinal = status.getStatus().ordinal();
            Assert.assertFalse(returnedStatus[ordinal]);
            returnedStatus[ordinal] = true;
        }
        
        Assert.assertEquals(10, numEntries);
    }
    */
    
    @SuppressWarnings("serial")
    private static class SkippedScoreGenerator extends ScoreGenerator {

        @Override
        public double generateScore(String domain, String pld, String url) {
            return ScoreGenerator.SKIP_SCORE;
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
    
    @SuppressWarnings("unchecked")
    private Lfs makeInputData(int numDomains, int numPages, Map<String, Comparable> metaData) throws IOException {
        Fields sfFields = UrlDatum.FIELDS.append(BaseDatum.makeMetaDataFields(metaData));
        Lfs in = new Lfs(new SequenceFile(sfFields), "build/test/FetchPipeLRTest/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        for (int i = 0; i < numDomains; i++) {
            for (int j = 0; j < numPages; j++) {
                // Use special domain name pattern so code deep inside of operations "knows" not
                // to try to resolve host names to IP addresses.
                write.add(makeTuple("bixo-test-domain-" + i + ".com", j, metaData));
            }
        }
        
        write.close();
        return in;
    }
    
    @SuppressWarnings("unchecked")
    private Lfs makeInputData(String domain, int numPages, Map<String, Comparable> metaData) throws IOException {
        Fields sfFields = UrlDatum.FIELDS.append(BaseDatum.makeMetaDataFields(metaData));
        Lfs in = new Lfs(new SequenceFile(sfFields), "build/test/FetchPipeLRTest/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        for (int j = 0; j < numPages; j++) {
            write.add(makeTuple(domain, j, metaData));
        }

        write.close();
        return in;
    }
    
    @SuppressWarnings("unchecked")
    private Tuple makeTuple(String domain, int pageNumber, Map<String, Comparable> metaData) {
        UrlDatum url = new UrlDatum("http://" + domain + "/page-" + pageNumber + ".html?size=10", 0, 0, UrlStatus.UNFETCHED, metaData);
        return url.toTuple();
    }
    
    @SuppressWarnings("serial")
    private static class NoRobotsResponseHandler extends RandomResponseHandler {

        public NoRobotsResponseHandler() {
            super(1000, 10);
        }
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            if (pathInContext.endsWith("/robots.txt")) {
                throw new HttpException(HttpStatus.SC_NOT_FOUND, "No robots.txt");
            } else {
                super.handle(pathInContext, pathParams, request, response);
            }
        }
    }
    
    /***********************************************************************
     * Lots of ugly custom classes to support serializable "mocking" for a
     * particular test case. Mockito mocks aren't serializable,
     * or at least I couldn't see an easy way to make this work.
     */

    @SuppressWarnings("serial")
    private static class CustomGrouper implements IGroupingKeyGenerator {

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
                return GroupingKey.makeGroupingKey(1, "domain-0.com", 30000);
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
        public FetcherPolicy makeNewPolicy(long crawlDelay) {
            return new MaxUrlFetcherPolicy(getMaxUrls());
        }
        
        @Override
        public int getMaxUrls() {
            return _maxUrls;
        }
    }
    
    @SuppressWarnings("serial")
    private static class CustomFetcher implements IHttpFetcher {

        @Override
        public FetchedDatum head(ScoredUrlDatum scoredUrl) throws BaseFetchException {
            throw new RuntimeException("Unexpected HEAD request");
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
        public byte[] get(String url) throws BaseFetchException {
            return null;
        }

        @Override
        public FetcherPolicy getFetcherPolicy() {
            return new MaxUrlFetcherPolicy(4);
        }

        @Override
        public int getMaxThreads() {
            return 1;
        }

		@Override
		public UserAgent getUserAgent() {
			return ConfigUtils.BIXO_TEST_AGENT;
		}
        
    };


    

}
