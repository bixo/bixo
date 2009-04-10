package bixo.fetcher.cascading;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import bixo.tuple.UrlTuple;
import cascading.scheme.SequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class DummyTap extends Tap {

    private Iterator _inputData;
    private ArrayList<Tuple> _outPut = new ArrayList<Tuple>();

    public DummyTap(Fields fields) {
        super(new SequenceFile(fields));
        System.out.println("some");
    }

    public DummyTap(List<Tuple> inputData, Fields fields) {
        super(new SequenceFile(fields));
        _inputData = inputData.iterator();
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
        return new Path("/somePath");
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
    public TupleEntryIterator openForRead(JobConf arg0) throws IOException {
        if (_inputData == null) {
            _inputData = _outPut.iterator();
        }
        return new TupleEntryIterator(UrlTuple.FIELDS, _inputData);
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
}
