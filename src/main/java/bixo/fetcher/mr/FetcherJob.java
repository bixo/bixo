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
package bixo.fetcher.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleComparator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;

public class FetcherJob {

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
