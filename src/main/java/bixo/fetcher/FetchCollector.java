package bixo.fetcher;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import bixo.items.FetchedContentItem;
import bixo.items.FetchedUrlItem;

import cascading.tuple.Tuple;

public class FetchCollector implements Closeable {
    public static final String STATUS_DIR_NAME = "status";
    public static final String CONTENT_DIR_NAME = "content";
    
    private TupleCollector _statusCollector;
    private TupleCollector _contentCollector;
    
    public FetchCollector(JobConf conf) {
        Path output = FileOutputFormat.getOutputPath(conf);
        FileSystem fs;
        
        try {
            fs = FileSystem.get(conf);
            fs.mkdirs(output);
            
            Path statusDir = new Path(output, STATUS_DIR_NAME);
            fs.mkdirs(statusDir);
            _statusCollector = new TupleCollector(SequenceFile.createWriter(fs, conf, new Path(statusDir, UUID.randomUUID().toString()),
                            Tuple.class, Tuple.class));
            
            
            Path contentDir = new Path(output, CONTENT_DIR_NAME);
            fs.mkdirs(contentDir);
            _contentCollector = new TupleCollector(SequenceFile.createWriter(fs, conf, new Path(contentDir, UUID.randomUUID().toString()),
                            Tuple.class, Tuple.class));
        } catch (IOException e) {
            throw new RuntimeException("Exception configuring", e);
        }
    }
    
    public void collect(FetchResult result) throws IOException {
        String url = result.getContent().getBaseUrl();
        FetchedUrlItem fetchedItem = new FetchedUrlItem(url, result.getStatusCode());
        _statusCollector.collect(new Tuple(), fetchedItem.toTuple());
        
        FetchedContentItem contentItem = new FetchedContentItem(url, result.getContent());
        _contentCollector.collect(new Tuple(), contentItem.toTuple());
    }

    @Override
    public void close() throws IOException {
        _statusCollector.close();
        _contentCollector.close();
    }
    
}
