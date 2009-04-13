package bixo.fetcher.cascading;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

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
import cascading.tap.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class FetchPipeTest {

    private static final long TEN_DAYS = 1000 * 60 * 60 * 24 * 10;

    @Test
    public void testFetchPipe() throws Exception {
        Pipe pipe = new Pipe("urlSource");
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcherFactory factory = new FakeHttpFetcherFactory(false, 10);
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, factory);

        Lfs in = new Lfs(new SequenceFile(UrlTuple.FIELDS), "build/test-data/FetchPipeTest/in", true);
        Lfs out = new Lfs(new SequenceFile(Fields.ALL), "build/test-data/FetchPipeTest/out", true);

//        TupleEntryCollector write = in.openForWrite(new JobConf());
//        for (int i = 0; i < 1000; i++) {
//            UrlTuple url = new UrlTuple();
//            url.setUrl("http://" + i);
//            url.setLastFetched(0);
//            url.setLastUpdated(0);
//            url.setLastStatus(FetchStatusCode.NEVER_FETCHED);
//            write.add(url.toTuple());
//        }
//        write.close();
        FlowConnector flowConnector = new FlowConnector();

        // Flow flow = flowConnector.connect(in, out, fetchPipe);
        // flow.complete();
        // TupleEntryIterator openSink = flow.openSink();
        // while (openSink.hasNext()) {
        // System.out.println(openSink.next());
        // }
        // now run this again and test the tap

        // Lfs dualLfs = new Lfs(new SequenceFile(Fields.ALL),
        // "build/test-data/FetchPipeTest/dual", true);
        FetchOutputTap outputTap = new FetchOutputTap("build/test-data/FetchPipeTest/dual", true);
        Flow flow = flowConnector.connect(in, outputTap, fetchPipe);
        flow.complete();

    }
}
