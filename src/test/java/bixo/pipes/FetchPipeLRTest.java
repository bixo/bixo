package bixo.pipes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.BaseDatum;
import bixo.datum.FetchedDatum;
import bixo.datum.GroupedUrlDatum;
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
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.fetcher.simulation.NullHttpFetcher;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.IScoreGenerator;
import bixo.fetcher.util.SimpleGroupingKeyGenerator;
import bixo.fetcher.util.SimpleScoreGenerator;
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
    
    @SuppressWarnings("serial")
    private static class SkippedScoreGenerator implements IScoreGenerator {

        @Override
        public double generateScore(GroupedUrlDatum urlTuple) {
            return IScoreGenerator.SKIP_URL_SCORE;
        }
    }
    
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
                UrlDatum url = new UrlDatum("http://domain-" + i + ".com/page-" + j + ".html?size=10", 0, 0, UrlStatus.UNFETCHED, metaData);
                Tuple tuple = url.toTuple();
                write.add(tuple);
            }
        }
        
        write.close();
        return in;
    }
    
    @Test
    public void testHeadersInStatus() throws Exception {
        Lfs in = makeInputData(1, 1);

        Pipe pipe = new Pipe("urlSource");
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 1);
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(new NullHttpFetcher(), true);
        IScoreGenerator scoring = new SimpleScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher);
        
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
    
    @Test
    public void testFetchPipe() throws Exception {
        Lfs in = makeInputData(25, 4);

        Pipe pipe = new Pipe("urlSource");
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(new NullHttpFetcher(), true);
        IScoreGenerator scoring = new RandomScoreGenerator(0.0, 1.0);
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 10);
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher);
        
        String outputPath = "build/test/FetchPipeTest/testFetchPipe";
        Tap status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status", true);
        Tap content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content", true);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        // Verify 100 fetched and 100 status entries were saved.
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        
        Fields metaDataFields = new Fields();
        int totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            FetchedDatum datum = new FetchedDatum(entry, metaDataFields);
            Assert.assertNotNull(datum.getBaseUrl());
            Assert.assertNotNull(datum.getFetchedUrl());
        }
                
        Assert.assertEquals(100, totalEntries);
        tupleEntryIterator.close();
        
        validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outputPath + "/status");
        tupleEntryIterator = validate.openForRead(new JobConf());
        totalEntries = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            totalEntries += 1;

            // Verify we can convert properly
            StatusDatum sd = new StatusDatum(entry, metaDataFields);
            Assert.assertEquals(UrlStatus.FETCHED, sd.getStatus());
        }
        
        Assert.assertEquals(100, totalEntries);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testMetaData() throws Exception {
        Map<String, Comparable> metaData = new HashMap<String, Comparable>();
        metaData.put("key", "value");
        Lfs in = makeInputData(1, 1, metaData);

        Pipe pipe = new Pipe("urlSource");
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 10);
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(new NullHttpFetcher(), true);
        IScoreGenerator scoring = new SimpleScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher, new Fields("key"));
        
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
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(new NullHttpFetcher(), true);
        IScoreGenerator scoring = new SkippedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher);
        
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
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(new NullHttpFetcher(), true);
        IScoreGenerator scoring = new SimpleScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher);

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
    
    /***********************************************************************
     * Lots of ugly custom classes to support serializable "mocking" for a
     * particular test case (which follows). Mockito mocks aren't serializable,
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
                return GroupingKey.makeGroupingKey("domain-0.com", 30000);
            }
        }
    };
    
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
    
    @SuppressWarnings("serial")
    private static class CustomFetcher implements IHttpFetcher {

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
    

}
