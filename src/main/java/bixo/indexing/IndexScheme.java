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

import bixo.datum.BaseDatum;
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
    
    private static final String ANALYZER_KEY = "bixo.indexer.analyzer";
    private static final String MAX_FIELD_LENGTH_KEY = "bixo.indexer.maxFieldLength";
    private static final String SINK_FIELDS_KEY = "bixo.indexer.sinkFields";
    private static final String HAS_BOOST_KEY = "bixo.indexer.hasBoost";
    private static final String INDEX_SETTINGS_KEY = "bixo.indexer.indexSettings";
    private static final String STORE_SETTINGS_KEY = "bixo.indexer.storeSettings";
    
    public static final String BOOST_FIELD = BaseDatum.fieldName(IndexScheme.class, "boost");
    public static final Fields BOOST_FIELDS = new Fields(BOOST_FIELD);

    private Class<? extends Analyzer> _analyzer;
    private int _maxFieldLength;
    private Store[] _storeSettings;
    private Index[] _indexSettings;
    private boolean _hasBoost;
    
    /**
     * Class to provide access to protected getTaskOutputPath on 0.18.3
     *
     */
    @SuppressWarnings("unchecked")
    private static class MyFileOutputFormat extends FileOutputFormat {

        @Override
        public RecordWriter getRecordWriter(FileSystem arg0, JobConf arg1, String arg2,
                        Progressable arg3) throws IOException {
            throw new RuntimeException("MyFileOutputFormat should only be used for getTaskOutputPath");
        }
        
        public static Path getTaskOutputPath(JobConf conf, String name) throws IOException {
            return FileOutputFormat.getTaskOutputPath(conf, name);
        }
    }
    
    public IndexScheme(Fields fieldsToIndex, Store[] storeSettings, Index[] indexSettings, Class<? extends Analyzer> analyzer, int maxFieldLength) {
        this(fieldsToIndex, storeSettings, indexSettings, false, analyzer, maxFieldLength);
    }
    
    public IndexScheme(Fields fieldsToIndex, Store[] storeSettings, Index[] indexSettings, boolean hasBoost, Class<? extends Analyzer> analyzer, int maxFieldLength) {
        if (fieldsToIndex.size() == 0) {
            throw new IllegalArgumentException("At least one field must be specified for indexing");
        }

        if ((fieldsToIndex.size() != storeSettings.length) || (fieldsToIndex.size() != indexSettings.length)) {
            throw new IllegalArgumentException("store[] and index[] need to have same length as fieldsToIndex");
        }
        
        if (hasBoost) {
            // Boost field has to come at end of tuple, and is always a float.
            setSinkFields(fieldsToIndex.append(new Fields(BOOST_FIELD)));
        } else {
            setSinkFields(fieldsToIndex);
        }
        
        _analyzer = analyzer;
        _maxFieldLength = maxFieldLength;
        _storeSettings = storeSettings;
        _indexSettings = indexSettings;
        _hasBoost = hasBoost;
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(IndexingOutputFormat.class);
        conf.set(SINK_FIELDS_KEY, Util.serializeBase64(getSinkFields()));
        conf.setBoolean(HAS_BOOST_KEY, _hasBoost);
        conf.setClass(ANALYZER_KEY, _analyzer, Analyzer.class);
        conf.setInt(MAX_FIELD_LENGTH_KEY, _maxFieldLength);
        conf.set(INDEX_SETTINGS_KEY, Util.serializeBase64(_indexSettings));
        conf.set(STORE_SETTINGS_KEY, Util.serializeBase64(_storeSettings));
        
        LOGGER.info("Initializing Lucene index tap");
        Fields fields = getSinkFields();
        for (int i = 0; i < fields.size() - 1; i++) {
            LOGGER.info("  Field " + fields.get(i) + ": " + _storeSettings[i] + ", " + _indexSettings[i]);
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

            Analyzer analyzer = (Analyzer) ReflectionUtils.newInstance(conf.getClass(ANALYZER_KEY, Analyzer.class), conf);
            final Index[] index = (Index[]) Util.deserializeBase64(conf.get(INDEX_SETTINGS_KEY));
            final Store[] store = (Store[]) Util.deserializeBase64(conf.get(STORE_SETTINGS_KEY));

            int maxFieldLength = conf.getInt(MAX_FIELD_LENGTH_KEY, MaxFieldLength.UNLIMITED.getLimit());
            final Fields sinkFields = (Fields)Util.deserializeBase64(conf.get(SINK_FIELDS_KEY));
            final boolean hasBoost = conf.getBoolean(HAS_BOOST_KEY, false);
            
            String tmpFolder = System.getProperty("java.io.tmpdir");
            final File localIndexFolder = new File(tmpFolder, UUID.randomUUID().toString());
            final IndexWriter indexWriter = new IndexWriter(localIndexFolder, analyzer, new MaxFieldLength(maxFieldLength));

            return new RecordWriter<Tuple, Tuple>() {

                @Override
                public void close(final Reporter reporter) throws IOException {
                    // Hadoop need to know we still working on it.
                    Thread reporterThread = new Thread() {
                        @Override
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
                    
                    LOGGER.info("Optimizing index for " + name);
                    indexWriter.optimize();
                    indexWriter.close();
                    // now we copy the local folder to the remote one and delete
                    // the older
                    Path dist = MyFileOutputFormat.getTaskOutputPath(conf, name);
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
                    if (hasBoost) {
                        size -= 1;
                    }
                    
                    for (int i = 0; i < size; i++) {
                        String name = (String) sinkFields.get(i);
                        Comparable comparable = value.get(i);
                        doc.add(new Field(name, "" + comparable.toString(), store[i], index[i]));
                    }
                    
                    // We append the boost field at the end, so it's always the last value in the tuple.
                    if (hasBoost) {
                        float boost = value.getFloat(size);
                        doc.setBoost(boost);
                    }
                    
                    indexWriter.addDocument(doc);
                }

            };
        }
    }
}
