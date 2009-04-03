package bixo.fetcher;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import bixo.items.FetchItem;
import bixo.items.UrlItem;

import cascading.tuple.Tuple;
import cascading.util.Util;

public class FetcherMapper implements Mapper<Tuple, Tuple, Tuple, Tuple> {
    public static final String GROUPING_KEY_GENERATOR = "groupingKeyGenerator";
    public static final String SCORE_GENERATOR = "scoreGenerator";
    
    private GroupingKeyGenerator _groupingKeyGenerator;
    private ScoreGenerator _scoreGenerator;

    @Override
    public void configure(JobConf conf) {
        try {
            _groupingKeyGenerator = (GroupingKeyGenerator) Util.deserializeBase64(conf.getRaw(GROUPING_KEY_GENERATOR));
            _scoreGenerator = (ScoreGenerator) Util.deserializeBase64(conf.getRaw(SCORE_GENERATOR));
        } catch (IOException e) {
            throw new RuntimeException("Unable to setup Fetcher", e);
        }
    }

    @Override
    public void map(Tuple key, Tuple value, OutputCollector<Tuple, Tuple> collector, Reporter reporter) throws IOException {
        // TODO KKr - try/catch so if (for example) we get a MalformedURLException we don't kill the job?
        UrlItem urlItem = new UrlItem(value);

        // get Grouping Key
        Tuple groupingKey = new Tuple(_groupingKeyGenerator.getGroupingKey(urlItem));

        // calculate a score,
        double score = _scoreGenerator.generateScore(urlItem);
        // create a fetchItem tuple.
        FetchItem fetchItem = new FetchItem(urlItem.getUrl(), score);
        collector.collect(groupingKey, fetchItem.toTuple());
    }

    @Override
    public void close() throws IOException {
    }

}
