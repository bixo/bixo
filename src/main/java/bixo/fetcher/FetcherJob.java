package bixo.fetcher;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import bixo.fetcher.impl.LastFetchScoreGenerator;
import bixo.fetcher.impl.PLDGrouping;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleComparator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;

public class FetcherJob {
    private static Logger LOG = Logger.getLogger(FetcherJob.class);

    public void run(String input, String output) throws IOException {

        JobConf job = new JobConf();
        job.setJobName("fetcher");
        job.setSpeculativeExecution(false);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(FetcherMapper.class);
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setReducerClass(FetcherReducer.class);
        job.setOutputKeyClass(Tuple.class);
        job.setOutputValueClass(Tuple.class);

        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        // since we use tuples we need custom serialization
        String serializations = job.get("io.serializations");
        job.set("io.serializations", Util.join(",", serializations, TupleSerialization.class.getName()));
        job.setOutputKeyComparatorClass(TupleComparator.class);

        // setup grouping and scoring
        PLDGrouping grouping = new PLDGrouping();
        job.set(FetcherMapper.GROUPING_KEY_GENERATOR, Util.serializeBase64(grouping));
        long crawlInterval = 10 * 24 * 60 * 60 * 1000; // every 10 days
        LastFetchScoreGenerator score = new LastFetchScoreGenerator(System.currentTimeMillis(), crawlInterval);
        job.set(FetcherMapper.SCORE_GENERATOR, Util.serializeBase64(score));

        // run job and block until job is done
        JobClient.runJob(job);
    }

}
