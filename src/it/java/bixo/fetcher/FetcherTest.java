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
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.config.FakeUserFetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.BaseFetchException;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.SimpleGroupingKeyGenerator;
import bixo.pipes.FetchPipe;
import bixo.urldb.UrlImporter;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class FetcherTest {
    private static final long TEN_DAYS = 1000 * 60 * 60 * 24 * 10;

	@SuppressWarnings("serial")
	private static class FirefoxUserAgent extends UserAgent {
		public FirefoxUserAgent() {
			super("Firefox", "", "");
		}
		
		@Override
		public String getUserAgentString() {
	    	// Use standard Firefox agent name, as some sites won't work w/non-standard names.
			return "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.8) Gecko/2009032608 Firefox/3.0.8";
		}
	}
	    
    private String makeUrlDB(String workingFolder, String inputPath) throws IOException {

        // We don't want to regenerate this DB all the time.
        if (!new File(workingFolder, UrlImporter.URL_DB_NAME).exists()) {
            UrlImporter urlImporter = new UrlImporter();
            FileUtil.fullyDelete(new File(workingFolder));
            urlImporter.importUrls(inputPath, workingFolder);
        }

        return workingFolder + "/" + UrlImporter.URL_DB_NAME;
    }
    
    @Test
    public void testStaleConnection() throws Exception {
        String workingFolder = "build/it/FetcherTest-testStaleConnection/working";
        String inputPath = makeUrlDB(workingFolder, "src/it/resources/facebook-artists.txt");
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), inputPath, true);
        String outPath = "build/it/FetcherTest-testStaleConnection/out";
        Lfs content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outPath + "/content", true);
        Lfs status = new Lfs(new SequenceFile(StatusDatum.FIELDS), outPath + "/status", true);
        
        Pipe pipe = new Pipe("urlSource");

        UserAgent userAgent = new FirefoxUserAgent();
        IHttpFetcher fetcher = new SimpleHttpFetcher(10, new FakeUserFetcherPolicy(5), userAgent);
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(fetcher, true);
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher);

        FlowConnector flowConnector = new FlowConnector();

        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
        flow.complete();
        
        // Test for all valid fetches.
        Lfs validate = new Lfs(new SequenceFile(StatusDatum.FIELDS), outPath + "/status");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        Fields metaDataFields = new Fields();
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            StatusDatum sd = new StatusDatum(entry, metaDataFields);
            if (sd.getStatus() != UrlStatus.FETCHED) {
            	System.out.println("Fetch failed for " + sd.getUrl());
            	BaseFetchException e = sd.getException();
            	if (e != null) {
            		System.out.println("Exception in status: " + e.getMessage());
            	}
            	Assert.fail("Status not equal to FETCHED");
            }
        }
    }

    @Test
    public void testRunFetcher() throws Exception {
        String workingFolder = "build/it/FetcherTest-run/working";
        String inputPath = makeUrlDB(workingFolder, "src/it/resources/top10urls.txt");
        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), inputPath, true);
        String outPath = workingFolder + "/FetcherTest-testRunFetcher";
        Lfs content = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outPath + "/content", true);

        Pipe pipe = new Pipe("urlSource");

        UserAgent userAgent = new FirefoxUserAgent();
        IHttpFetcher fetcher = new SimpleHttpFetcher(10, userAgent);
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(fetcher, true);
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher);

        FlowConnector flowConnector = new FlowConnector();

        Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(null, content), fetchPipe);
        flow.complete();
        
        // Test for 10 good fetches.
        Lfs validate = new Lfs(new SequenceFile(FetchedDatum.FIELDS), outPath + "/content");
        TupleEntryIterator tupleEntryIterator = validate.openForRead(new JobConf());
        Fields metaDataFields = new Fields();
        int fetchedPages = 0;
        while (tupleEntryIterator.hasNext()) {
            TupleEntry entry = tupleEntryIterator.next();
            new FetchedDatum(entry, metaDataFields);
            fetchedPages += 1;
        }

        Assert.assertEquals(10, fetchedPages);
    }
    
}
