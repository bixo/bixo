package bixo.cascading;

import java.io.File;

import org.junit.Test;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class HelloWorldCascadingTest extends ClusterTestCase {

  public HelloWorldCascadingTest() {
    super("hello world", true);
  }

  @Test
  public void testHello() throws Exception {

    String inputPath = "src/test-data/sometext.txt";
    if (!new File(inputPath).exists()) {
      fail("data file not found");
    }
    String outputPath = "build/test-data/HelloWorldCascadingTest/testHello";
    new File(outputPath).mkdirs();

    copyFromLocal(inputPath);

    Scheme sourceScheme = new TextLine(new Fields("line"));
    Tap source = new Hfs(sourceScheme, inputPath);

    Scheme sinkScheme = new TextLine(new Fields("word", "count"));
    Tap sink = new Hfs(sinkScheme, outputPath, true);

    Pipe assembly = new Pipe("wordcount");

    String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
    Function function = new RegexGenerator(new Fields("word"), regex);
    assembly = new Each(assembly, new Fields("line"), function);

    assembly = new GroupBy(assembly, new Fields("word"));
    Aggregator count = new Count(new Fields("count"));
    assembly = new Every(assembly, count);

    FlowConnector flowConnector = new FlowConnector();
    Flow flow = flowConnector.connect("word-count", source, sink, assembly);
    flow.complete();

    TupleEntryIterator resultSink = flow.openSink();
    TupleEntry t = null;
    while (resultSink.hasNext()) {
      TupleEntry tupleEntry = (TupleEntry) resultSink.next();
      System.out.println(tupleEntry);

    }
    validateLength(flow, 173, null);
  }

}
