package bixo.indexing;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class IndexSchemaTest {

    @SuppressWarnings({ "unchecked", "serial" })
    public static class CalcBoost extends BaseOperation implements Function {

        public CalcBoost() {
            super(IndexScheme.BOOST_FIELDS);
        }
        
        @Override
        public void operate(FlowProcess process, FunctionCall funcCall) {
            TupleEntry entry = funcCall.getArguments();
            TupleEntryCollector collector = funcCall.getOutputCollector();

            String value = entry.getString("parsed-content");
            String[] values = value.split(" ");
            float boostValue = Float.parseFloat(values[2]);
            TupleEntry boost = new TupleEntry(IndexScheme.BOOST_FIELDS, new Tuple(boostValue));
            collector.add(boost);
        }
    }
    
    @Test
    public void testIndexSink() throws Exception {
        String out = "build/test/IndexSchemaTest/testIndexSink/out";

        Lfs lfs = new Lfs(new IndexScheme(new Fields("text"), new Store[] { Store.NO }, new Index[] { Index.NOT_ANALYZED }, StandardAnalyzer.class, MaxFieldLength.UNLIMITED.getLimit()), out,
                        SinkMode.REPLACE);
        TupleEntryCollector writer = lfs.openForWrite(new JobConf());

        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple("some text"));
        }

        writer.close();

    }

    @Test
    public void testPipeIntoIndex() throws Exception {

        String in = "build/test/IndexSchemaTest/testPipeIntoIndex/in";

        Lfs lfs = new Lfs(new SequenceFile(ParsedDatum.FIELDS), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        for (int i = 0; i < 10; i++) {
            float boost = 0.5f + (float)i/5.0f;
            ParsedDatum resultTuple = new ParsedDatum("http://" + i, "test " + i + " " + boost, "title-" + i, new Outlink[0], null);
            write.add(resultTuple.toTuple());
        }
        write.close();

        Fields indexFields = new Fields("parsed-content");
        Store[] storeSettings = new Store[] { Store.YES };
        Index[] indexSettings = new Index[] { Index.ANALYZED };
        IndexScheme scheme = new IndexScheme(indexFields, storeSettings, indexSettings, true, StandardAnalyzer.class, MaxFieldLength.UNLIMITED.getLimit());

        // Rename the fields from what we've got, to what the Lucene field names should be.
        Pipe indexingPipe = new Pipe("parse importer");
        Fields parseFields = new Fields(ParsedDatum.PARSED_TEXT_FIELD);
        indexingPipe = new Each(indexingPipe, parseFields, new Identity(indexFields));

        // We want to inject in the boost field. So we'll pass everything to
        // the CalcBoost(), and what CalcBoost outputs will be merged in with ALL of the input fields.
        indexingPipe = new Each(indexingPipe, new CalcBoost(), Fields.ALL);
        
        String out = "build/test/IndexSchemaTest/testPipeIntoIndex/out";
        FileUtil.fullyDelete(new File(out));
        Lfs indexSinkTap = new Lfs(scheme, out, SinkMode.REPLACE);
        
        Flow flow = new FlowConnector().connect(lfs, indexSinkTap, indexingPipe);
        flow.complete();

        File file = new File(out);
        File[] listFiles = file.listFiles();
        IndexReader[] indexReaders = new IndexReader[listFiles.length];

        for (int i = 0; i < listFiles.length; i++) {
            File indexFile = listFiles[i];
            indexReaders[i] = IndexReader.open(indexFile);
        }

        QueryParser parser = new QueryParser("parsed-content", new StandardAnalyzer());
        IndexSearcher searcher = new IndexSearcher(new MultiReader(indexReaders));
        for (int i = 0; i < 10; i++) {
            TopDocs search = searcher.search(parser.parse("" + i), 1);
            
            Assert.assertEquals(1, search.totalHits);
        }
        
        // the top hit should be the last document we added, which will have
        // the max boost.
        TopDocs hits = searcher.search(parser.parse("test"), 1);
        Assert.assertEquals(10, hits.totalHits);
        ScoreDoc[] docs = hits.scoreDocs;
        Document doc = searcher.doc(docs[0].doc);
        String content = doc.get("parsed-content");
        Assert.assertTrue(content.startsWith("test 9"));
    }
}
