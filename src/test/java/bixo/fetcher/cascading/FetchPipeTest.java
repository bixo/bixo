package bixo.fetcher.cascading;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.fetcher.beans.FetchStatusCode;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.tuple.UrlTuple;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class FetchPipeTest {

    private static final long TEN_DAYS = 1000 * 60 * 60 * 24 * 10;

    @Test
    public void testFetchPipe() throws Exception {
        Pipe pipe = new Pipe("urlSource");
        FetchPipe fetchPipe = new FetchPipe(pipe, new PLDGrouping(), new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS));

        ArrayList<Tuple> list = new ArrayList<Tuple>();
//        Lfs lfs = new Lfs(new SequenceFile(UrlTuple.FIELDS), "/someInputUrls");
//        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        for (int i = 0; i < 1000; i++) {
            UrlTuple url = new UrlTuple("http://" + i, 0, 0, FetchStatusCode.NEVER_FETCHED);
//            write.add(url.toTuple());
            list.add(url.toTuple());
        }
//        write.close();
        Tap source = new DummyTap(list, UrlTuple.FIELDS);
        Tap sink = new DummyTap(UrlTuple.FIELDS);

        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(source, sink, fetchPipe);
        flow.complete();
        TupleEntryIterator openSink = flow.openSink();
        while (openSink.hasNext()) {
            System.out.println(openSink.next());
        }
    }
}
