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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import cascading.util.Util;

public class IndexScheme extends Scheme {
    private static final String ANALYZER = "bixo.analyzer";
    private static final String MAX_FIELD_LENGTH = "bix.maxFieldLength";
    private static final String SINK_FIELDS = "bixo.fields";
    private Class<? extends Analyzer> _analyzer;
    private int _maxFieldLeng;

    public IndexScheme(Fields fieldsToIndex, Class<? extends Analyzer> analyzer, int maxFieldLength) {
        if (fieldsToIndex.size() == 0) {
            throw new IllegalArgumentException("At least one field need to be specified by name");
        }
        setSinkFields(fieldsToIndex);
        _analyzer = analyzer;
        _maxFieldLeng = maxFieldLength;
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(IndexingOutputFormat.class);
        conf.set(SINK_FIELDS, Util.serializeBase64(getSinkFields()));
        conf.setClass(ANALYZER, _analyzer, Analyzer.class);
        conf.setInt(MAX_FIELD_LENGTH, _maxFieldLeng);

    }

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

            int maxFieldLength = conf.getInt(MAX_FIELD_LENGTH, MaxFieldLength.UNLIMITED.getLimit());
            final Fields sinkFields = (Fields) Util.deserializeBase64(conf.get(SINK_FIELDS));

            String tmpFolder = System.getProperty("java.io.tmpdir");
            final File localIndexFolder = new File(tmpFolder, UUID.randomUUID().toString());
            final IndexWriter indexWriter = new IndexWriter(localIndexFolder, analyzer, new MaxFieldLength(maxFieldLength));

            return new RecordWriter<Tuple, Tuple>() {

                @Override
                public void close(final Reporter reporter) throws IOException {
                    // hadoop need to know we still working on it.
                    Thread reporterThread = new Thread() {
                        public void run() {
                            while (!isInterrupted()) {
                                reporter.progress();
                                try {
                                    sleep(10 * 1000);
                                } catch (InterruptedException e) {
                                }
                            }
                        }
                    };
                    reporterThread.start();
                    //
                    indexWriter.optimize();
                    indexWriter.close();
                    // now we copy the local folder to the remote one and delete
                    // the older
                    Path dist = FileOutputFormat.getTaskOutputPath(conf, name);
                    FileSystem dstFS = dist.getFileSystem(conf);
                    dstFS.copyFromLocalFile(true, new Path(localIndexFolder.getAbsolutePath()), dist);
                    // we are done and stop the reporter
                    reporterThread.interrupt();
                }

                @Override
                public void write(Tuple key, Tuple value) throws IOException {
                    Document doc = new Document();
                    int size = sinkFields.size();
                    for (int i = 0; i < size; i++) {
                        String name = (String) sinkFields.get(i);
                        Comparable comparable = value.get(i);
                        // TODO sg we want to make all this configurable...
                        doc.add(new Field(name, "" + comparable.toString(), Field.Store.YES, Field.Index.TOKENIZED));
                    }
                    indexWriter.addDocument(doc);
                }

            };
        }
    }
}
