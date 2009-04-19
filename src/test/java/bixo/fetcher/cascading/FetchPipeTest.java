package bixo.fetcher.cascading;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.IConstants;
import bixo.cascading.MultiSinkTap;
import bixo.fetcher.FakeHttpFetcherFactory;
import bixo.fetcher.IHttpFetcherFactory;
import bixo.fetcher.beans.FetchStatusCode;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.tuple.UrlTuple;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

public class FetchPipeTest {

    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;

    @Test
    public void testFetchPipe() throws Exception {

        // First create a sequence file with 1000 UrlTuple tuples in it.
        Lfs in = new Lfs(new SequenceFile(UrlTuple.FIELDS), "build/test-data/FetchPipeTest/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        for (int i = 0; i < 1000; i++) {
            UrlTuple url = new UrlTuple();
            url.setUrl("http://" + i);
            url.setLastFetched(0);
            url.setLastUpdated(0);
            url.setLastStatus(FetchStatusCode.NEVER_FETCHED);
            write.add(url.toTuple());
        }
        write.close();

        // Create the fetch pipe we'll use to process these fake URLs
        Pipe pipe = new Pipe("urlSource");
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcherFactory factory = new FakeHttpFetcherFactory(false, 10);
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, factory);

        // Test that we correctly generated the sequence file.
        // Flow flow = flowConnector.connect(in, out, fetchPipe);
        // flow.complete();
        // TupleEntryIterator openSink = flow.openSink();
        // while (openSink.hasNext()) {
        // System.out.println(openSink.next());
        // }
        // now run this again and test the tap

        // Lfs dualLfs = new Lfs(new SequenceFile(Fields.ALL),
        // "build/test-data/FetchPipeTest/dual", true);

        // Create the output, which is a dual file sink tap.
        String outputPath = "build/test-data/FetchPipeTest/dual";
        Tap status = new Hfs(new TextLine(new Fields(IConstants.URL, IConstants.FETCH_STATUS), new Fields(IConstants.URL, IConstants.FETCH_STATUS)), outputPath + "/status", true);
        Tap content = new Hfs(new TextLine(new Fields(IConstants.URL, IConstants.CONTENT), new Fields(IConstants.URL, IConstants.FETCH_CONTENT)), outputPath + "/content", true);
        Tap sink = new MultiSinkTap(status, content);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, sink, fetchPipe);
        flow.complete();
    }
}
