package bixo.pipes;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import bixo.cascading.NullContext;
import bixo.cascading.Payload;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.fetcher.util.FixedScoreGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.indexing.IndexScheme;
import bixo.parser.FakeParser;
import bixo.utils.DomainInfo;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.PlannerException;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class IndexingMetaDataTest {
    private static final int DATA_COUNT = 100;

    private static final Fields INDEXED_FIELDS = new Fields("text", "payload");

    @SuppressWarnings("serial")
    private static class ExtractIndexingFields extends BaseOperation<NullContext> implements Function<NullContext> {
    
        public ExtractIndexingFields() {
            super(INDEXED_FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            ParsedDatum datum = new ParsedDatum(funcCall.getArguments());
            Tuple result = new Tuple(datum.getParsedText(), datum.getPayload().get("payload"));
            funcCall.getOutputCollector().add(result);
        }
    }
    
    @Test
    public void testMetaData() throws Exception {
        // we want to pass meta data all the way through to the index.

        Lfs in = new Lfs(new SequenceFile(UrlDatum.FIELDS), "build/test/TestMetaData/testMetaData/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());

        for (int i = 0; i < DATA_COUNT; i++) {
            Payload payload = new Payload();
            payload.put("payload", "payload" + i);
            UrlDatum url = new UrlDatum("http://" + DomainInfo.makeTestDomain(i));
            url.setPayload(payload);
            write.add(url.getTuple());
        }
        write.close();

        Pipe pipe = new Pipe("urlSource");
        IHttpFetcher fetcher = new FakeHttpFetcher(false, DATA_COUNT);

        ScoreGenerator scorer = new FixedScoreGenerator();
        FetchPipe fetchPipe = new FetchPipe(pipe, scorer, fetcher, fetcher, null, 1);
        ParsePipe parserPipe = new ParsePipe(fetchPipe.getContentTailPipe(), new FakeParser());

        Pipe indexPipe = new Each(parserPipe, new ExtractIndexingFields());
        
        String out = "build/test/TestMetaData/testMetaData/out";
        FileUtil.fullyDelete(new File(out));
        Lfs indexSinkTap = new Lfs(new IndexScheme(INDEXED_FIELDS, new Store[] { Store.NO, Store.NO },
                        new Index[] { Index.NOT_ANALYZED, Index.NOT_ANALYZED }, KeywordAnalyzer.class,
                        MaxFieldLength.UNLIMITED.getLimit()), out, SinkMode.REPLACE);
        try {
            Flow flow = new FlowConnector().connect(in, indexSinkTap, indexPipe);
            flow.complete();
        } catch (PlannerException e) {
            e.writeDOT("build/failedFlow.dot");
            throw e;
        }

        File file = new File(out);
        File[] listFiles = file.listFiles();
        IndexReader[] indexReaders = new IndexReader[listFiles.length];

        for (int i = 0; i < listFiles.length; i++) {
            File indexFile = listFiles[i];
            indexReaders[i] = IndexReader.open(indexFile);
        }

        QueryParser parser = new QueryParser("payload", new KeywordAnalyzer());
        IndexSearcher searcher = new IndexSearcher(new MultiReader(indexReaders));
        for (int i = 0; i < DATA_COUNT; i++) {
            TopDocs search = searcher.search(parser.parse("payload" + i), 1);
            Assert.assertEquals("Didn't find payload" + i, 1, search.totalHits);
        }
    }
}
