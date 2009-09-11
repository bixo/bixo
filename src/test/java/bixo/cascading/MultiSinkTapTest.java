package bixo.cascading;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class MultiSinkTapTest extends CascadingTestCase {

    @Test
    public void testMultiTap() throws Exception {

        String in = "build/test/MultiSinkTapTest/testMultiTap/in";
        Lfs lfs = new Lfs(new TextLine(), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        for (int i = 0; i < 10; i++) {
            Tuple tuple = new Tuple("left" + i, "right" + i, "content" + i);
            System.out.println("MultiSinkTapTest.testMultiTap()" + tuple);
            write.add(tuple);
        }
        write.close();

        Tap source = new Hfs(new TextLine(new Fields("line")), in);
        Pipe pipe = new Pipe("test");
        pipe = new Each(pipe, new RegexSplitter(new Fields("left", "right", "content"), "\t"));
        String outputPath = "build/test/MultiSinkTapTest/testMultiTap/out";
        Tap lhsSink = new Hfs(new TextLine(new Fields("key", "value"), new Fields("left", "content")), outputPath + "/multisink/lhs", true);
        Tap rhsSink = new Hfs(new TextLine(new Fields("key", "value"), new Fields("right", "content")), outputPath + "/multisink/rhs", true);

        Tap sink = new MultiSinkTap(lhsSink, rhsSink);

        Flow flow = new FlowConnector().connect(source, sink, pipe);

        flow.complete();

        TupleEntryIterator left = flow.openTapForRead(lhsSink);
        while (left.hasNext()) {
            System.out.println("this is left:" + left.next());

        }
        TupleEntryIterator right = flow.openTapForRead(rhsSink);

        while (right.hasNext()) {
            System.out.println("this is right:" + right.next());

        }
        // validateLength(openTapForRead, 10);
        // validateLength(flow.openTapForRead(rhsSink), 10);
    }

}
