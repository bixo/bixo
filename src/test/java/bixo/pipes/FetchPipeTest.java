package bixo.pipes;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.cascading.MultiSinkTap;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.FakeHttpFetcher;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.pipes.FetchPipe;
import bixo.urldb.IUrlNormalizer;
import bixo.urldb.UrlNormalizer;
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

        // First create a sequence file with 1000 UrlDatum tuples in it.
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), "build/test-data/FetchPipeTest/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        for (int i = 0; i < 1000; i++) {
            UrlDatum url = new UrlDatum("http://" + i, 0, 0, FetchStatusCode.UNFETCHED, null);
            write.add(url.toTuple());
        }
        write.close();

        // Create the fetch pipe we'll use to process these fake URLs
        Pipe pipe = new Pipe("urlSource");
        IUrlNormalizer urlNormalizer = new UrlNormalizer();
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcher fetcher = new FakeHttpFetcher(false, 10);
        FetchPipe fetchPipe = new FetchPipe(pipe, urlNormalizer, grouping, scoring, fetcher);

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
        Tap status = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD)), outputPath + "/status", true);
        Tap content = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD)), outputPath + "/content", true);
        Tap sink = new MultiSinkTap(status, content);

        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, sink, fetchPipe);
        flow.complete();
    }
}
