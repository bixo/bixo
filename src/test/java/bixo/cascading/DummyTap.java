package bixo.cascading;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.flow.Flow;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.hadoop.TupleSerialization;

@SuppressWarnings("serial")
public class DummyTap extends Tap {

    private List<Tuple> _inputData;
    private ArrayList<Tuple> _outPut = new ArrayList<Tuple>();
//    private String _id;

    public DummyTap() {
    }

    public DummyTap(List<Tuple> inputData, Fields fields) {
        super(new MyScheme());
//        _id = UUID.randomUUID().toString();
        _inputData = inputData;
    }

    @Override
    public void flowInit(Flow flow) {
        JobConf conf = flow.getJobConf();
        Path qualifiedPath = new Path("/somePathWeDontUseAnyhow");
        for (Path exitingPath : FileInputFormat.getInputPaths(conf)) {
            if (exitingPath.equals(qualifiedPath))
                throw new TapException("may not add duplicate paths, found: " + exitingPath);
        }

        FileInputFormat.addInputPath(conf, qualifiedPath);

        TupleSerialization.setSerializations(conf); // allows Hfs to be used
        // independent of Flow
    }

    @Override
    public boolean isWriteDirect() {
        return true;
    }

    @Override
    public boolean deletePath(JobConf arg0) throws IOException {
        return true;
    }

    @Override
    public Path getPath() {
        return new Path("/somePath" + System.currentTimeMillis());
    }

    @Override
    public long getPathModified(JobConf arg0) throws IOException {
        return 0;
    }

    @Override
    public boolean makeDirs(JobConf arg0) throws IOException {
        return true;
    }

    @Override
    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
        if (_inputData == null) {
            _inputData = _outPut;
        }
        
        // TODO SGr - not sure what to use for fields here. Do we even need
        // this DummyTap class?
        return new TupleEntryIterator(Fields.ALL, _inputData.iterator());
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf arg0) throws IOException {
        return new TupleEntryCollector() {
            @Override
            protected void collect(Tuple tuple) {
                _outPut.add(tuple);

            }

        };
    }

    @Override
    public boolean pathExists(JobConf arg0) throws IOException {
        return true;
    }

    public ArrayList<Tuple> getOutPut() {
        return _outPut;
    }

    @Override
	public boolean equals(Object object) {
        if (object instanceof DummyTap) {
            DummyTap other = (DummyTap) object;
            return this.toString().equals(other.toString());
        }
        return false;
    }

    private static class MyScheme extends Scheme {

        @SuppressWarnings("unchecked")
        @Override
        public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void sinkInit(Tap tap, JobConf conf) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public Tuple source(Object key, Object value) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void sourceInit(Tap tap, JobConf conf) throws IOException {
            // TODO Auto-generated method stub

        }

    }
}
