package bixo.indexing;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import cascading.util.Util;

@SuppressWarnings("serial")
public class IndexScheme extends Scheme {
    private static final Logger LOGGER = Logger.getLogger(IndexScheme.class);
    
    private static final String ANALYZER = "bixo.analyzer";
    private static final String MAX_FIELD_LENGTH = "bixo.maxFieldLength";
    private static final String SINK_FIELDS = "bixo.fields";
    private static final String INDEX = "bixo.index";
    private static final String STORE = "bixo.store";
    
    private Class<? extends Analyzer> _analyzer;
    private int _maxFieldLeng;
    private Store[] _store;
    private Index[] _index;

    public IndexScheme(Fields fieldsToIndex, Store[] store, Index[] index, Class<? extends Analyzer> analyzer, int maxFieldLength) {
        if (fieldsToIndex.size() == 0) {
            throw new IllegalArgumentException("At least one field need to be specified by name");
        }

        if ((fieldsToIndex.size() != store.length) || (fieldsToIndex.size() != index.length)) {
            throw new IllegalArgumentException("store[] and index[] need to have same length as fieldsToIndex");
        }
        
        setSinkFields(fieldsToIndex);
        _analyzer = analyzer;
        _maxFieldLeng = maxFieldLength;
        _store = store;
        _index = index;
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(IndexingOutputFormat.class);
        conf.set(SINK_FIELDS, Util.serializeBase64(getSinkFields()));
        conf.setClass(ANALYZER, _analyzer, Analyzer.class);
        conf.setInt(MAX_FIELD_LENGTH, _maxFieldLeng);
        conf.set(INDEX, Util.serializeBase64(_index));
        conf.set(STORE, Util.serializeBase64(_store));
        
        LOGGER.info("Initializing Lucene index tap");
        Fields fields = getSinkFields();
        for (int i = 0; i < fields.size(); i++) {
            LOGGER.info("  Field " + fields.get(i) + ": " + _store[i] + ", " + _index[i]);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        Tuple result = getSinkFields() != null ? tupleEntry.selectTuple(getSinkFields()) : tupleEntry.getTuple();
        outputCollector.collect(Tuples.NULL, result);
    }

    @Override
    public Tuple source(Object key, Object value) {
        throw new UnsupportedOperationException("This is sink only scheme");
    }

    @Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
        throw new UnsupportedOperationException("This is sink only scheme");

    }

    public static class IndexingOutputFormat implements OutputFormat<Tuple, Tuple> {

        @Override
        public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
        }

        @Override
        public RecordWriter<Tuple, Tuple> getRecordWriter(final FileSystem fs, final JobConf conf, final String name, Progressable progressable) throws IOException {

            Analyzer analyzer = (Analyzer) ReflectionUtils.newInstance(conf.getClass(ANALYZER, Analyzer.class), conf);
            final Index[] index = (Index[]) Util.deserializeBase64(conf.get(INDEX));
            final Store[] store = (Store[]) Util.deserializeBase64(conf.get(STORE));

            int maxFieldLength = conf.getInt(MAX_FIELD_LENGTH, MaxFieldLength.UNLIMITED.getLimit());
            final Fields sinkFields = (Fields) Util.deserializeBase64(conf.get(SINK_FIELDS));

            String tmpFolder = System.getProperty("java.io.tmpdir");
            final File localIndexFolder = new File(tmpFolder, UUID.randomUUID().toString());
            final IndexWriter indexWriter = new IndexWriter(localIndexFolder, analyzer, new MaxFieldLength(maxFieldLength));

            return new RecordWriter<Tuple, Tuple>() {

                @Override
                public void close(final Reporter reporter) throws IOException {
                    // Hadoop need to know we still working on it.
                    Thread reporterThread = new Thread() {
                        public void run() {
                            while (!isInterrupted()) {
                                reporter.progress();
                                try {
                                    sleep(10 * 1000);
                                } catch (InterruptedException e) {
                                    interrupt();
                                }
                            }
                        }
                    };
                    reporterThread.start();
                    
                    //
                    LOGGER.info("Optimizing index for " + name);
                    indexWriter.optimize();
                    indexWriter.close();
                    // now we copy the local folder to the remote one and delete
                    // the older
                    Path dist = FileOutputFormat.getTaskOutputPath(conf, name);
                    FileSystem dstFS = dist.getFileSystem(conf);
                    LOGGER.info("Copying index from local to " + dist);
                    dstFS.copyFromLocalFile(true, new Path(localIndexFolder.getAbsolutePath()), dist);
                    // we are done and stop the reporter
                    reporterThread.interrupt();
                }

                @SuppressWarnings("unchecked")
                @Override
                public void write(Tuple key, Tuple value) throws IOException {
                    Document doc = new Document();
                    int size = sinkFields.size();
                    for (int i = 0; i < size; i++) {
                        String name = (String) sinkFields.get(i);
                        Comparable comparable = value.get(i);
                        doc.add(new Field(name, "" + comparable.toString(), store[i], index[i]));
                    }
                    indexWriter.addDocument(doc);
                }

            };
        }
    }
}
