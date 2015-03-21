/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.examples.crawl;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;
import bixo.datum.UrlDatum;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;


public class LatestUrlDatumBufferTest {

    private static final String WORKINGDIR = "build/test/LatestUrlDatumBufferTest";

    @Before
    public void setUp() throws IOException {
        
        File workingFolder = new File(WORKINGDIR);
        if (workingFolder.exists()) {
            FileUtils.deleteDirectory(workingFolder);
        }
    }
 
/*  Can't use the test below since it doesn't simulate the reusing of tuples in a Cascading
 *  GroupBy operation. 
 *  In particular it will fail to catch a case where an assignment of the type 
 *      aDatum = datum 
 *  is being incorrectly done.
 *  Instead we want it to be 
 *      aDatum = new DatumType(datum)
*/
    /*
    @Test
    public void testOperate() throws BaseFetchException, IOException {
        LatestUrlDatumBuffer op = new LatestUrlDatumBuffer();
        
        HadoopFlowProcess fp = Mockito.mock(HadoopFlowProcess.class);
        Mockito.when(fp.getJobConf()).thenReturn(new JobConf());

        OperationCall<NullContext> oc = Mockito.mock(OperationCall.class);
        BufferCall<NullContext> bc = Mockito.mock(BufferCall.class);
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);

        List<TupleEntry> tupleEntryList = new ArrayList<TupleEntry>();
        UrlDatum urlDatum1 = new UrlDatum("http://foo.com");
        urlDatum1.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 0L);
        urlDatum1.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, UrlStatus.UNFETCHED);
        TupleEntry entry1 = new TupleEntry(UrlDatum.FIELDS);
        entry1.setTuple(urlDatum1.getTuple());
        tupleEntryList.add(entry1);

        UrlDatum urlDatum2 = new UrlDatum("http://foo.com");
        urlDatum2.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 2L);
        urlDatum2.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, UrlStatus.FETCHED);
        TupleEntry entry2 = new TupleEntry(UrlDatum.FIELDS);
        entry2.setTuple(urlDatum2.getTuple());
        tupleEntryList.add(entry2);

        UrlDatum urlDatum3 = new UrlDatum("http://foo.com");
        urlDatum3.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 0L);
        urlDatum3.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, UrlStatus.UNFETCHED);
        TupleEntry entry3 = new TupleEntry(UrlDatum.FIELDS);
        entry3.setTuple(urlDatum3.getTuple());
        tupleEntryList.add(entry3);
        Mockito.when(bc.getArgumentsIterator()).thenReturn(tupleEntryList.iterator());
        Mockito.when(bc.getOutputCollector()).thenReturn(collector);

        op.prepare(fp, oc);
        op.operate(fp, bc);
        op.cleanup(fp, oc);

        Mockito.verify(collector, Mockito.times(1)).add(Mockito.argThat(new MatchUrlDatum()));
        Mockito.verifyNoMoreInteractions(collector);

    }
    
    private static class MatchUrlDatum extends ArgumentMatcher<Tuple> {

        @Override
        public boolean matches(Object argument) {
            TupleEntry entry = new TupleEntry(UrlDatum.FIELDS);
            entry.setTuple((Tuple)argument);
            UrlDatum datum = new UrlDatum(entry);
            Long expectedVal = new Long(2);
            Long result = (Long)datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
            if (result.longValue() == expectedVal.longValue()) {
                return true;
            }
            return false;
        }
    }
 
 */
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testOperateWithGroupBy() throws Exception {
        
        BixoPlatform platform = new BixoPlatform(LatestUrlDatumBufferTest.class, Platform.Local);
        
        // Create a temp file with a fetched url
        BasePath workingDirPath = platform.makePath(WORKINGDIR);
        BasePath fetchedDatumsPath = platform.makePath(workingDirPath, "fetched");
        ArrayList<UrlDatum> fetchedDatums = new ArrayList<UrlDatum>();
        UrlDatum fetchedDatum1 = new UrlDatum("http://foo.com");
        fetchedDatum1.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 2L);
        fetchedDatums.add(fetchedDatum1);
        createDataFile(platform, fetchedDatumsPath, fetchedDatums);
        
        // And another with unfetched urls
        BasePath unfetchedDatumsPath = platform.makePath(workingDirPath, "unfetched");
        ArrayList<UrlDatum> unfetchedDatums = new ArrayList<UrlDatum>();
        UrlDatum unfetchedDatum1 = new UrlDatum("http://foo.com");
        unfetchedDatum1.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 0L);
        unfetchedDatums.add(unfetchedDatum1);
        UrlDatum unfetchedDatum2 = new UrlDatum("http://foo.com");
        unfetchedDatum2.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 0L);
        unfetchedDatums.add(unfetchedDatum2);
        
        createDataFile(platform, unfetchedDatumsPath, unfetchedDatums);

        
        // create a workflow
        Tap inputSource1 = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), fetchedDatumsPath);
        Pipe fetchedPipe = new Pipe("fetched");
        Tap inputSource2 = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), unfetchedDatumsPath);
        Pipe unfetchedPipe = new Pipe("unfetched");

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(fetchedPipe.getName(), inputSource1);
        sources.put(unfetchedPipe.getName(), inputSource2);

        BasePath resultsPath = platform.makePath(workingDirPath, "results");
        Tap resultSink = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), resultsPath, SinkMode.REPLACE);

        Pipe resultsPipe = new GroupBy("results pipe", Pipe.pipes(fetchedPipe, unfetchedPipe), 
                        new Fields(UrlDatum.URL_FN));
        resultsPipe = new Every(resultsPipe, new LatestUrlDatumBuffer(), Fields.RESULTS);


        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(sources, resultSink, resultsPipe);
        flow.complete();
        
        // verify that the resulting pipe has the latest tuple
        
        Tap testSink = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), resultsPath);
        TupleEntryIterator reader = testSink.openForRead(platform.makeFlowProcess());
        int count = 0;
        long latest = 0;
        while (reader.hasNext()) {
            TupleEntry next = reader.next();
            UrlDatum datum = new UrlDatum(next);
            latest = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
            count++;
        }
        
        assertEquals(1, count);
        assertEquals(2, latest);

        
        
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void createDataFile(BasePlatform platform, BasePath filePath, List<UrlDatum> datums) throws Exception {
        Tap urlSink = platform.makeTap(platform.makeBinaryScheme(UrlDatum.FIELDS), filePath, SinkMode.REPLACE);
        TupleEntryCollector writer = urlSink.openForWrite(platform.makeFlowProcess());
        for (UrlDatum datum : datums) {
            writer.add(datum.getTuple());
        }
        writer.close();
    }
}
