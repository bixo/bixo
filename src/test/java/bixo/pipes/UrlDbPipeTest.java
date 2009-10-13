package bixo.pipes;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.urldb.SimpleUrlFilter;
import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.MultiTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class UrlDbPipeTest extends CascadingTestCase {

    @Test
    public void testUrlDbPipe() throws Exception {

        Lfs in1 = new Lfs(new SequenceFile(UrlDatum.FIELDS), "build/test/UrlDbPipeTest/in1", true);
        Lfs in2 = new Lfs(new SequenceFile(UrlDatum.FIELDS), "build/test/UrlDbPipeTest/in2", true);
        Lfs in3 = new Lfs(new SequenceFile(UrlDatum.FIELDS), "build/test/UrlDbPipeTest/in3", true);

        Tap out = new Lfs(new SequenceFile(UrlDatum.FIELDS), "build/test/UrlDbPipeTest/out", true);

        TupleEntryCollector write1 = in1.openForWrite(new JobConf());

        // existing URLs
        for (int i = 0; i < 100; i++) {
            String url = "http://domain.com/page-" + i;
            long lastUpdated = System.currentTimeMillis();
            long lastFetched = System.currentTimeMillis();
            UrlDatum datum = new UrlDatum(url, lastFetched, lastUpdated, UrlStatus.UNFETCHED, null);
            write1.add(datum.toTuple());
        }

        // some status updates
        TupleEntryCollector write2 = in2.openForWrite(new JobConf());

        for (int i = 0; i < 100; i++) {
            String url = "http://domain.com/page-" + i;
            long lastUpdated = System.currentTimeMillis();
            long lastFetched = System.currentTimeMillis();
            UrlDatum datum = new UrlDatum(url, lastFetched, lastUpdated, UrlStatus.UNFETCHED, null);
            write2.add(datum.toTuple());
        }
        TupleEntryCollector write3 = in3.openForWrite(new JobConf());
        // some fresh URLs
        for (int i = 100; i < 200; i++) {
            String url = "http://domain.com/page-" + i;
            long lastUpdated = System.currentTimeMillis();
            long lastFetched = System.currentTimeMillis();
            UrlDatum datum = new UrlDatum(url, lastFetched, lastUpdated, UrlStatus.UNFETCHED, null);
            write3.add(datum.toTuple());
        }

        write1.close();
        write2.close();
        write3.close();

        Pipe pipe = new Pipe("urlDb_source");

        UrlDbPipe urlDbPipe = new UrlDbPipe(pipe, new SimpleUrlFilter(), new Fields());
        FlowConnector flowConnector = new FlowConnector();
        Tap input = new MultiTap(in1, in2, in3);

        Flow flow = flowConnector.connect(input, out, urlDbPipe);
        flow.complete();
//        validateLength(flow, 200);
        TupleEntryIterator read = out.openForRead(new JobConf());
        while (read.hasNext()) {
            TupleEntry next = read.next();
            System.out.println(next.toString());
        }
        read.close();
    }

}
