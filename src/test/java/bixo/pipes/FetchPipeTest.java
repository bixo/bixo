package bixo.pipes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

import bixo.cascading.MultiSinkTap;
import bixo.config.FetcherPolicy;
import bixo.datum.BaseDatum;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.FakeHttpFetcher;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.operations.FetcherBuffer;
import bixo.pipes.FetchPipe;
import bixo.urldb.IUrlNormalizer;
import bixo.urldb.UrlNormalizer;
import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;

public class FetchPipeTest extends CascadingTestCase {

    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;

    private Lfs makeInputData(int numDomains, int numPages) throws IOException {
        return makeInputData(numDomains, numPages, null);
    }
    
    @SuppressWarnings("unchecked")
    private Lfs makeInputData(int numDomains, int numPages, Map<String, Comparable> metaData) throws IOException {
        Fields sfFields = UrlDatum.FIELDS.append(BaseDatum.makeMetaDataFields(metaData));
        Lfs in = new Lfs(new SequenceFile(sfFields), "build/test-data/FetchPipeTest/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        for (int i = 0; i < numDomains; i++) {
            for (int j = 0; j < numPages; j++) {
                UrlDatum url = new UrlDatum("http://domain-" + i + ".com/page-" + j + ".html?size=10", 0, 0, FetchStatusCode.UNFETCHED, metaData);
                Tuple tuple = url.toTuple();
                write.add(tuple);
            }
        }
        
        write.close();
        return in;
    }
    
    @Test
    public void testFetchPipe() throws Exception {
        Lfs in = makeInputData(100, 1);

        Pipe pipe = new Pipe("urlSource");
        IUrlNormalizer urlNormalizer = new UrlNormalizer();
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 10);
        FetchPipe fetchPipe = new FetchPipe(pipe, urlNormalizer, grouping, scoring, fetcher);
        
        String outputPath = "build/test-data/FetchPipeTest/dual";
        Tap status = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD)), outputPath + "/status", true);
        Tap content = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD)), outputPath + "/content", true);
        Tap sink = new MultiSinkTap(status, content);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, sink, fetchPipe);
        flow.complete();
    }
    
    @Test
    public void testDurationLimit() throws Exception {
        // Pretend like we have 10 URLs from the same domain
        Lfs in = makeInputData(1, 10);

        // Create the fetch pipe we'll use to process these fake URLs
        Pipe pipe = new Pipe("urlSource");
        IUrlNormalizer urlNormalizer = new UrlNormalizer();
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 1);
        FetchPipe fetchPipe = new FetchPipe(pipe, urlNormalizer, grouping, scoring, fetcher);

        // Create the output
        String outputPath = "build/test-data/FetchPipeTest/out";
        Tap out = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath, true);

        // Finally we can run it. Set up our prefs with a FetcherPolicy that has an end time of now,
        // which means we should get only a single URL from our one domain.
        Properties properties = new Properties();
        FetcherPolicy defaultPolicy = new FetcherPolicy();
        defaultPolicy.setCrawlEndTime(System.currentTimeMillis());
        properties.put(FetcherBuffer.DEFAULT_FETCHER_POLICY_KEY, Util.serializeBase64(defaultPolicy));

        FlowConnector flowConnector = new FlowConnector(properties);
        Flow flow = flowConnector.connect(in, out, fetchPipe);
        flow.complete();
        
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outputPath);
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        validateLength(tupleEntryIterator, 1);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testMetaData() throws Exception {
        Map<String, Comparable> metaData = new HashMap<String, Comparable>();
        metaData.put("key", "value");
        Lfs in = makeInputData(1, 1, metaData);

        Pipe pipe = new Pipe("urlSource");
        IUrlNormalizer urlNormalizer = new UrlNormalizer();
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 10);
        FetchPipe fetchPipe = new FetchPipe(pipe, urlNormalizer, grouping, scoring, fetcher, new Fields("key"));
        
        String outputPath = "build/test-data/FetchPipeTest/dual";
        Fields contentFields = FetchedDatum.FIELDS.append(new Fields("key"));
        Tap status = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD)), outputPath + "/status", true);
        Tap content = new Hfs(new SequenceFile(contentFields), outputPath + "/content", true);
        Tap sink = new MultiSinkTap(status, content);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, sink, fetchPipe);
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
    

}
