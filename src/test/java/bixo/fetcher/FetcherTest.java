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
package bixo.fetcher;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import bixo.datum.BaseDatum;
import bixo.datum.IFieldNames;
import bixo.fetcher.http.HttpClientFactory;
import bixo.fetcher.http.IHttpFetcherFactory;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.pipes.FetchPipe;
import bixo.urldb.UrlImporter;
import bixo.utils.TimeStampUtil;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

public class FetcherTest {
    private static final long TEN_DAYS = 1000 * 60 * 60 * 24 * 10;

    @Test
    public void testRunFetcher() throws Exception {
        // create url db
        String workingFolder = "build/test-data/FetcherTest/working";

        // we might dont want to regenerate that all the time..
        if (!new File(workingFolder, IFieldNames.URL_DB).exists()) {
            UrlImporter urlImporter = new UrlImporter();
            String inputPath = "src/test-data/top10urls.txt";
            FileUtil.fullyDelete(new File(workingFolder));
            urlImporter.importUrls(inputPath, workingFolder);
        }

        String inputPath = workingFolder + "/" + IFieldNames.URL_DB;
        Lfs in = new Lfs(new SequenceFile(BaseDatum.FIELDS), inputPath, true);
        String outPath = workingFolder + "/" + IFieldNames.FETCH + TimeStampUtil.nowWithUnderLine();
        Lfs out = new Lfs(new SequenceFile(Fields.ALL), outPath, true);

        Pipe pipe = new Pipe("urlSource");

        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcherFactory factory = new HttpClientFactory(10);
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, factory);

        FlowConnector flowConnector = new FlowConnector();

        Flow flow = flowConnector.connect(in, out, fetchPipe);
        flow.complete();
        TupleEntryIterator openSink = flow.openSink();
        while (openSink.hasNext()) {
            System.out.println(openSink.next());
        }

    }
}
