package bixo.examples;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.BaseFetchException;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.bixolabs.cascading.HadoopUtils;
import com.bixolabs.cascading.NullContext;

public class LatestUrlDatumBufferTest {

    private static final String WORKINGDIR = "build/test/LatestUrlDatumBufferTest";
    private static final Path _workingDirPath = new Path(WORKINGDIR);
    private JobConf _conf = new JobConf();

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
    
    
    @Test
    public void testOperateWithGroupBy() throws IOException {
        
        // Create a temp file with a fetched url
        Path fetchedDatumsPath = new Path(_workingDirPath, "fetched");
        ArrayList<UrlDatum> fetchedDatums = new ArrayList<UrlDatum>();
        UrlDatum fetchedDatum1 = new UrlDatum("http://foo.com");
        fetchedDatum1.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 2L);
        fetchedDatums.add(fetchedDatum1);
        createDataFile(fetchedDatumsPath.toString(), fetchedDatums);
        
        // And another with unfetched urls
        Path unfetchedDatumsPath = new Path(_workingDirPath, "unfetched");
        ArrayList<UrlDatum> unfetchedDatums = new ArrayList<UrlDatum>();
        UrlDatum unfetchedDatum1 = new UrlDatum("http://foo.com");
        unfetchedDatum1.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 0L);
        unfetchedDatums.add(unfetchedDatum1);
        UrlDatum unfetchedDatum2 = new UrlDatum("http://foo.com");
        unfetchedDatum2.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, 0L);
        unfetchedDatums.add(unfetchedDatum2);
        
        createDataFile(unfetchedDatumsPath.toString(), unfetchedDatums);

        
        // create a workflow
        Tap inputSource1 = new Hfs(new SequenceFile(UrlDatum.FIELDS), fetchedDatumsPath.toString());
        Pipe fetchedPipe = new Pipe("fetched");
        Tap inputSource2 = new Hfs(new SequenceFile(UrlDatum.FIELDS), unfetchedDatumsPath.toString());
        Pipe unfetchedPipe = new Pipe("unfetched");

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(fetchedPipe.getName(), inputSource1);
        sources.put(unfetchedPipe.getName(), inputSource2);

        Path resultsPath = new Path(_workingDirPath, "results");
        Tap resultSink = new Hfs(new SequenceFile(UrlDatum.FIELDS), resultsPath.toString(), true);

        Pipe resultsPipe = new GroupBy("results pipe", Pipe.pipes(fetchedPipe, unfetchedPipe), 
                        new Fields(UrlDatum.URL_FN));
        resultsPipe = new Every(resultsPipe, new LatestUrlDatumBuffer(), Fields.RESULTS);

        Properties props = HadoopUtils.getDefaultProperties(LatestUrlDatumBufferTest.class, false, _conf);

        FlowConnector flowConnector = new FlowConnector(props);
        Flow flow = flowConnector.connect(sources, resultSink, resultsPipe);
        flow.complete();
        
        // verify that the resulting pipe has the latest tuple
        
        Tap testSink = new Hfs(new SequenceFile(UrlDatum.FIELDS), resultsPath.toString(), false);
        TupleEntryIterator reader = testSink.openForRead(_conf);
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
    
    private void createDataFile(String fileName, List<UrlDatum> datums) throws IOException {
        Tap urlSink = new Hfs(new SequenceFile(UrlDatum.FIELDS), fileName, true);
        TupleEntryCollector writer = urlSink.openForWrite(_conf);
        for (UrlDatum datum : datums) {
            writer.add(datum.getTuple());
        }
        writer.close();
    }
}
