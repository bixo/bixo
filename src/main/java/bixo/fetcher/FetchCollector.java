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
