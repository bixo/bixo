package bixo.fetcher.cascading;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import bixo.Constants;
import bixo.fetcher.beans.FetchStatusCode;
import bixo.tuple.FetchContentTuple;
import bixo.tuple.FetchResultTuple;
import bixo.tuple.UrlTuple;
import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapCollector;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuples;

@SuppressWarnings("serial")
public class FetchOutputTap extends Hfs {

    public FetchOutputTap(String path, boolean overwrite) {
        super(new MySchema(Fields.ALL), path, overwrite);
    }

    @Override
    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
        throw new UnsupportedOperationException("This is a write only tap");
    }

    @Override
    public boolean isWriteDirect() {
        return true;
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf conf) throws IOException {

        String statusPath = new Path(getPath(), Constants.STATUS).toUri().toString();
        Hfs status = new Hfs(getScheme(), statusPath, isReplace());
        status.sinkInit(conf);
        TapCollector statusCollector = new TapCollector(status, conf);
        
        String contentPath = new Path(getPath(), Constants.CONTENT).toUri().toString();
        Hfs content = new Hfs(getScheme(), contentPath, isReplace());
        content.sinkInit(conf);
        TapCollector contentCollector = new TapCollector(content, conf);
        

        return new DualTupleEntryCollector(statusCollector, contentCollector);

    }

    public static class DualTupleEntryCollector extends TupleEntryCollector implements OutputCollector {

        private final TapCollector _statusCollector;
        private final TapCollector _contentCollector;

        public DualTupleEntryCollector(TapCollector statusCollector, TapCollector contentCollector) {
            _statusCollector = statusCollector;
            _contentCollector = contentCollector;
        }

        @Override
        protected void collect(Tuple tuple) {
            FetchResultTuple fetchResultTuple = new FetchResultTuple(tuple);

            FetchStatusCode statusCode = fetchResultTuple.getStatusCode();
            FetchContentTuple content = fetchResultTuple.getContent();
            _statusCollector.add(new UrlTuple(content.getFetchedUrl(), content.getFetchTime(), System.currentTimeMillis(), statusCode).toTuple());
            _contentCollector.add(content.toTuple());
        }

        @Override
        public void close() {
            _statusCollector.close();
            _contentCollector.close();
        }

        @Override
        public void collect(Object key, Object value) throws IOException {
            collect((Tuple) value);
        }
    }

    public static class MySchema extends SequenceFile {

        public MySchema(Fields all) {
            super(all);

        }

        @Override
        public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
            Tuple result = getSinkFields() != null ? tupleEntry.selectTuple(getSinkFields()) : tupleEntry.getTuple();
            outputCollector.collect(Tuples.NULL, result);
        }

    }
}
