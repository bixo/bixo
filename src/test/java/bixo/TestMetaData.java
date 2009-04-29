package bixo;

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

import bixo.datum.BaseDatum;
import bixo.datum.FetchStatusCode;
import bixo.datum.UrlDatum;
import bixo.fetcher.FakeHttpFetcherFactory;
import bixo.fetcher.http.IHttpFetcherFactory;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.indexing.IndexScheme;
import bixo.parser.ParserPipeTest.FakeParserFactor;
import bixo.pipes.FetchPipe;
import bixo.pipes.ParserPipe;
import bixo.util.FieldUtil;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class TestMetaData {
    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;

    @Test
    public void testMetaData() throws Exception {
        // we want to pass meta data all the way through to the index.

        Lfs in = new Lfs(new SequenceFile(FieldUtil.combine(BaseDatum.FIELDS, "metaData")), "build/test-data/TestMetaData/testMetaData/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        for (int i = 0; i < 1000; i++) {
            UrlDatum url = new UrlDatum();
            url.setUrl("http://" + i);
            url.setLastFetched(0);
            url.setLastUpdated(0);
            url.setLastStatus(FetchStatusCode.NEVER_FETCHED);
            Tuple tuple = url.toTuple();
            tuple.add("metaData" + i);
            write.add(tuple);
        }
        write.close();

        Pipe pipe = new Pipe("urlSource");
        PLDGrouping grouping = new PLDGrouping();
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcherFactory factory = new FakeHttpFetcherFactory(false, 10);

        Fields metaDataField = new Fields("metaData");

        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, factory, metaDataField);

        ParserPipe parserPipe = new ParserPipe(fetchPipe, new FakeParserFactor(), metaDataField);

        String out = "build/test-data/TestMetaData/testMetaData/out";
        FileUtil.fullyDelete(new File(out));
        Lfs indexSinkTap = new Lfs(new IndexScheme(new Fields("text", "metaData"), new Store[] { Store.NO, Store.NO }, new Index[] { Index.NOT_ANALYZED, Index.NOT_ANALYZED }, KeywordAnalyzer.class,
                        MaxFieldLength.UNLIMITED.getLimit()), out, SinkMode.REPLACE);

        Flow flow = new FlowConnector().connect(in, indexSinkTap, parserPipe);
        flow.complete();

        File file = new File(out);
        File[] listFiles = file.listFiles();
        IndexReader[] indexReaders = new IndexReader[listFiles.length];

        for (int i = 0; i < listFiles.length; i++) {
            File indexFile = listFiles[i];
            indexReaders[i] = IndexReader.open(indexFile);
        }

        QueryParser parser = new QueryParser("metadata", new KeywordAnalyzer());
        IndexSearcher searcher = new IndexSearcher(new MultiReader(indexReaders));
        for (int i = 0; i < 10000; i++) {
            TopDocs search = searcher.search(parser.parse("metaData" + i), 1);
            Assert.assertEquals(1, search.totalHits);
        }
    }
}
