package bixo.fetcher;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import bixo.fetcher.impl.LastFetchScoreGenerator;
import bixo.fetcher.impl.PLDGrouping;
import bixo.items.FetchItem;
import bixo.items.FetchedItem;
import bixo.items.UrlItem;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleComparator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;

public class Fetcher implements Mapper<Tuple, Tuple, Tuple, Tuple>, Reducer<Tuple, Tuple, Tuple, Tuple> {

    private static final String GROUPING_KEY_GENERATOR = "groupingKeyGenerator";
    private static final String SCORE_GENERATOR = "scoreGenerator";
    private GroupingKeyGenerator _groupingKeyGenerator;
    private ScoreGenerator _scoreGenerator;
    private Worker[] _workers;
    private ArrayBlockingQueue<ValueContainer> _queue;
    // Special end-of-stream marker. If a worker retrieves
    private static final ValueContainer NO_MORE_WORK = new Fetcher.ValueContainer(null, null);

    private static Logger LOG = Logger.getLogger(Fetcher.class);

    public void run(String input, String output) throws IOException {

        JobConf job = new JobConf();
        job.setJobName("fetcher");
        job.setSpeculativeExecution(false);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(Fetcher.class);
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setReducerClass(Fetcher.class);
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
        job.set(GROUPING_KEY_GENERATOR, Util.serializeBase64(grouping));
        long crawlInterval = 10 * 24 * 60 * 60 * 1000; // every 10 days
        LastFetchScoreGenerator score = new LastFetchScoreGenerator(System.currentTimeMillis(), crawlInterval);
        job.set(SCORE_GENERATOR, Util.serializeBase64(score));

        // run job and block until job is done
        JobClient.runJob(job);
    }

    @Override
    public void configure(JobConf conf) {
        // MAPPER
        try {
            _groupingKeyGenerator = (GroupingKeyGenerator) Util.deserializeBase64(conf.getRaw(GROUPING_KEY_GENERATOR));
            _scoreGenerator = (ScoreGenerator) Util.deserializeBase64(conf.getRaw(SCORE_GENERATOR));
        } catch (IOException e) {
            throw new RuntimeException("Unable to setup Fetcher", e);
        }
        // REDUCER
        final int capacity = 10;
        _queue = new ArrayBlockingQueue<ValueContainer>(capacity);

        // Create a set of worker threads
        final int numWorkers = 2;
        _workers = new Worker[numWorkers];
        for (int i = 0; i < _workers.length; i++) {
            _workers[i] = new Worker(_queue);
            _workers[i].start();
        }

    }

    @Override
    public void map(Tuple key, Tuple value, OutputCollector<Tuple, Tuple> collector, Reporter reporter) throws IOException {

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
    public void reduce(Tuple key, Iterator<Tuple> values, OutputCollector<Tuple, Tuple> collector, Reporter reporter) throws IOException {
        try {
            // Add some work to the queue; block if the queue is full.
            // Note that null cannot be added to a blocking queue.
            _queue.put(new ValueContainer(values, collector));

            // Add special end-of-stream markers to terminate the workers
        } catch (InterruptedException e) {
            throw new IOException("Unable to add to the working queue.", e);
        }
    }

    static class ValueContainer {
        private final Iterator<Tuple> _values;
        private final OutputCollector<Tuple, Tuple> _collector;

        public ValueContainer(Iterator<Tuple> values, OutputCollector<Tuple, Tuple> collector) {
            _values = values;
            _collector = collector;
        }
    }

    class Worker extends Thread {

        BlockingQueue<ValueContainer> _queue;

        Worker(BlockingQueue<ValueContainer> queue) {
            _queue = queue;
        }

        public void run() {
            try {
                while (true) {
                    // Retrieve an integer; block if the queue is empty
                    ValueContainer container = _queue.take();

                    // Terminate if the end-of-stream marker was retrieved
                    if (container == NO_MORE_WORK) {
                        break;
                    }
                    Random random = new Random();

                    // int foudnUrls = random.nextInt(100);
                    Iterator<Tuple> values = container._values;
                    while (values.hasNext()) {
                        FetchItem fetchItem = new FetchItem(values.next());

                        int sleepTime = random.nextInt(30 * 1000);
                        sleep(sleepTime);
                        FetchedItem fetchedItem = new FetchedItem(fetchItem.getUrl(), new String());
                        try {
                            container._collector.collect(new Tuple(), fetchedItem.toTuple());
                        } catch (IOException e) {
                            LOG.error("Unable to collect fetchedItem", e);
                        }
                    }
                }
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < _workers.length; i++) {
            try {
                _queue.put(NO_MORE_WORK);
            } catch (InterruptedException e) {
                throw new IOException("Unable to send No more work signal. ", e);
            }
        }
    }

}
