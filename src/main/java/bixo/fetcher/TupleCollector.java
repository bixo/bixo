package bixo.fetcher;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.tuple.Tuple;

public class TupleCollector implements OutputCollector<Tuple, Tuple>, Closeable {
    private SequenceFile.Writer _writer;
    
    public TupleCollector(SequenceFile.Writer writer) {
        _writer = writer;
    }
    
    @Override
    public void collect(Tuple key, Tuple value) throws IOException {
        _writer.append(key, value);
        
    }

    @Override
    public void close() throws IOException {
        _writer.close();
    }

}
