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
package bixo.urldb;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import bixo.Constants;
import bixo.HadoopConfigured;
import bixo.tuple.UrlTuple;
import bixo.utils.TimeStampUtil;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Last;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.MultiTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class UrlImporter extends HadoopConfigured {

    public void importUrls(String inputPath, String workingFolder) throws IOException {

        FileSystem fs = getFileSystem(workingFolder);
        Path currentDb = new Path(workingFolder, Constants.URL_DB);
        boolean dbexists = fs.exists(currentDb);
        // if db exists we want to merge dbs

        Path newDb = new Path(workingFolder, Constants.URL_DB + "-new-" + TimeStampUtil.nowWithUnderLine());
        Tap importSink = new Hfs(new SequenceFile(UrlTuple.FIELDS), newDb.toUri().toASCIIString(), true);
        // create tmp db
        importUrls(inputPath, importSink);

        if (dbexists) {
            // merge both together

            Tap oldDbTap = new Hfs(new SequenceFile(UrlTuple.FIELDS), workingFolder + "/" + Constants.URL_DB);

            Tap newDbTap = new Hfs(new SequenceFile(UrlTuple.FIELDS), newDb.toUri().toASCIIString());

            MultiTap source = new MultiTap(oldDbTap, newDbTap);

            Path mergeDb = new Path(workingFolder, Constants.URL_DB + "-merged-" + TimeStampUtil.nowWithUnderLine());
            Tap mergeSink = new Hfs(new SequenceFile(UrlTuple.FIELDS), mergeDb.toUri().toASCIIString(), true);

            Pipe pipe = new Pipe("urldb-merge");
            // we want the url with the latest update.
            pipe = new GroupBy(pipe, new Fields(Constants.URL));
            //
            Aggregator last = new LastUpdated(Constants.URL_TUPLE_VALUES);
            pipe = new Every(pipe, Constants.URL_TUPLE_VALUES, last);

            FlowConnector flowConnector = new FlowConnector();
            Flow flow = flowConnector.connect(source, mergeSink, pipe);
            flow.complete();

            Path oldDb = new Path(workingFolder, Constants.URL_DB + "-old-" + TimeStampUtil.nowWithUnderLine());

            fs.rename(currentDb, oldDb);
            fs.rename(mergeDb, currentDb);

            fs.delete(newDb, true);// remove the new db
            fs.delete(oldDb, true);
        } else {
            fs.rename(newDb, currentDb);
        }

    }

    public void importUrls(String inputPath, Tap sink) throws IOException {

        FileSystem fs = getFileSystem(inputPath);
        if (!fs.exists(new Path(inputPath))) {
            throw new IOException("data file not found");
        }

        // first parse the text file
        Scheme sourceScheme = new TextLine(new Fields("line"));
        Tap source = new Hfs(sourceScheme, inputPath);

        Pipe assembly = new Pipe("url-import");

        Function function = new TextUrlParser(null);
        assembly = new Each(assembly, new Fields("line"), function);

        assembly = new GroupBy(assembly, Constants.URL_TUPLE_KEY);
        // make sure we only have the url once.
        Last last = new Last(Constants.URL_TUPLE_VALUES);
        assembly = new Every(assembly, Constants.URL_TUPLE_VALUES, last);

        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect("url-import", source, sink, assembly);
        flow.complete();
    }

}
