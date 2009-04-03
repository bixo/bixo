package bixo.fetcher;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;

import cascading.tuple.Tuple;

public class TupleCollector implements OutputCollector<Tuple, Tuple> {
    
    @Override
    public void collect(Tuple key, Tuple value) throws IOException {
        // TODO Auto-generated method stub
        
    }

}
