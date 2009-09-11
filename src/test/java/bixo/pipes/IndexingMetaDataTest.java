package bixo.pipes;

import java.io.File;
import java.util.HashMap;

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

import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.fetcher.simulation.NullHttpFetcher;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.SimpleGroupingKeyGenerator;
import bixo.indexing.IndexScheme;
import bixo.parser.FakeParser;
import bixo.utils.FieldUtil;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.PlannerException;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

public class IndexingMetaDataTest {
    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;
    private static final int DATA_COUNT = 100;
    private static final String USER_AGENT_FAKE_FETCHING = "user agent for fake fetching";

    @SuppressWarnings("unchecked")
    @Test
    public void testMetaData() throws Exception {
        // we want to pass meta data all the way through to the index.

        Lfs in = new Lfs(new SequenceFile(FieldUtil.combine(UrlDatum.FIELDS, new Fields("metaData"))), "build/test/TestMetaData/testMetaData/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());

        for (int i = 0; i < DATA_COUNT; i++) {
            HashMap<String, Comparable> map = new HashMap<String, Comparable>();
            map.put("metaData", "metaData" + i);
            UrlDatum url = new UrlDatum("http://" + i, 0, 0, UrlStatus.UNFETCHED, map);
            write.add(url.toTuple());
        }
        write.close();

        Pipe pipe = new Pipe("urlSource");
        SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(USER_AGENT_FAKE_FETCHING, new NullHttpFetcher(), false);
        LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
        IHttpFetcher fetcher = new FakeHttpFetcher(false, DATA_COUNT);

        Fields metaDataField = new Fields("metaData");

        FetchPipe fetchPipe = new FetchPipe(pipe, grouping, scoring, fetcher, metaDataField);

        ParserPipe parserPipe = new ParserPipe(fetchPipe.getTailPipe(FetchPipe.FETCHED_PIPE_NAME), new FakeParser(), metaDataField);

        Fields indexedFields = new Fields("text", "metaData");
        Pipe indexPipe = new Each(parserPipe, new Fields(ParsedDatum.PARSED_TEXT_FIELD, "metaData"), new Identity(indexedFields));
        
        String out = "build/test/TestMetaData/testMetaData/out";
        FileUtil.fullyDelete(new File(out));
        Lfs indexSinkTap = new Lfs(new IndexScheme(indexedFields, new Store[] { Store.NO, Store.NO }, new Index[] { Index.NOT_ANALYZED, Index.NOT_ANALYZED }, KeywordAnalyzer.class,
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

        QueryParser parser = new QueryParser("metaData", new KeywordAnalyzer());
        IndexSearcher searcher = new IndexSearcher(new MultiReader(indexReaders));
        for (int i = 0; i < DATA_COUNT; i++) {
            TopDocs search = searcher.search(parser.parse("metaData" + i), 1);
            Assert.assertEquals(1, search.totalHits);
        }
    }
}
