/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
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

// Long-running test
public class HelloWorldCascadingLRTest extends ClusterTestCase {

  public HelloWorldCascadingLRTest() {
    super("hello world", true);
  }

  @Test
  public void testHello() throws Exception {

    String inputPath = "src/test/resources/sometext.txt";
    if (!new File(inputPath).exists()) {
      fail("data file not found");
    }
    String outputPath = "build/test/HelloWorldCascadingTest/testHello";
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
      // System.out.println(tupleEntry);
    }

    validateLength(flow, 173, null);
  }

}
