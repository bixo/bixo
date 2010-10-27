package bixo.operations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.cascading.PartitioningKey;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchSetDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.FetchRequest;
import bixo.robots.BaseRobotRules;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * We get ScoredUrlDatums, grouped by server IP address.
 * 
 * We need to generate sets of URLs to fetch, using a kept-alive connection.
 * Our output thus is one or more FetchSetDatums.
 *
 */
@SuppressWarnings( { "serial", "unchecked" })
public class MakeFetchSetsBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(MakeFetchSetsBuffer.class);

    private static final int URLS_PER_SKIPPED_BATCH = 100;
    
    private FetcherPolicy _fetcherPolicy;
    private int _numReduceTasks;
    private transient Random _rand;
    
    public MakeFetchSetsBuffer(FetcherPolicy fetcherPolicy, int numReduceTasks) {
        super(FetchSetDatum.FIELDS);

        _fetcherPolicy = fetcherPolicy;
        _numReduceTasks = numReduceTasks;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        _rand = new Random();
    }
    
    @Override
    public void operate(FlowProcess process, BufferCall buffCall) {
        Iterator<TupleEntry> values = buffCall.getArgumentsIterator();
        TupleEntry group = buffCall.getGroup();

        // <key> is the output of the IGroupingKeyGenerator used. This should
        // be <IP address>-<crawl delay in ms>
        String key = group.getString(0);

        if (GroupingKey.isSpecialKey(key)) {
            throw new RuntimeException("Invalid grouping key: " + key);
        }

        long crawlDelay = GroupingKey.getCrawlDelayFromKey(key);
        if (crawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) {
            crawlDelay = _fetcherPolicy.getCrawlDelay();
        }
        
        int maxUrls = _fetcherPolicy.getMaxUrlsPerServer();
        int totalUrls = 0;
        
        TupleEntryCollector collector = buffCall.getOutputCollector();

        PartitioningKey newKey = new PartitioningKey(key, _numReduceTasks);
        
        // We use the entire range (0...MAX_VALUE) for request time, as all that it's used for
        // is merge-sorting all of the FetchSetDatums.
        long curRequestTime = 0;
        long nextRequestTime = curRequestTime;

        int targetSize = 0;
        final long timeRangeDivisor = 1000;
        
        List<ScoredUrlDatum> urls = new LinkedList<ScoredUrlDatum>();
        
        boolean skipping = false;
        while (values.hasNext()) {
            if (targetSize == 0) {
                skipping = totalUrls >= maxUrls;
                // Figure out the max # of URLs that we would want to get.
                if (skipping) {
                    // We need to be skipping URLs. Do them in big chunks, and set the time to be
                    // the same for each (don't care, FetchBuffer has to handle skipping them).
                    targetSize = URLS_PER_SKIPPED_BATCH;
                    nextRequestTime = curRequestTime;
                } else {
                    curRequestTime = randRequestTime(_rand, timeRangeDivisor, curRequestTime);
                    FetchRequest request = _fetcherPolicy.getFetchRequest(curRequestTime, crawlDelay, Integer.MAX_VALUE);
                    targetSize = Math.min(request.getNumUrls(), maxUrls - totalUrls);
                    nextRequestTime = request.getNextRequestTime();
                }
            }

            ScoredUrlDatum scoredDatum = new ScoredUrlDatum(new TupleEntry(values.next()));
            urls.add(scoredDatum);
            totalUrls += 1;

            if (urls.size() >= targetSize) {
                LOGGER.trace(String.format("Added %d urls for ref %s in group %d at %d", urls.size(), newKey.getRef(), newKey.getValue(), curRequestTime));
                FetchSetDatum datum = new FetchSetDatum(urls, curRequestTime, nextRequestTime - curRequestTime, newKey.getValue(), newKey.getRef());
                
                // We're the last real set (for fetching purposes) if either there are no more URLs, or we're skipping URLs.
                datum.setLastList(!values.hasNext() || skipping);
                datum.setSkipped(skipping);
                collector.add(datum.getTuple());

                curRequestTime = nextRequestTime;
                urls.clear();
                targetSize = 0;
            }
        }
        
        // See if we have another partially built datum to add.
        if (urls.size() > 0) {
            LOGGER.trace(String.format("Added %d urls for ref %s in group %d at %d", urls.size(), newKey.getRef(), newKey.getValue(), curRequestTime));
            FetchSetDatum datum = new FetchSetDatum(urls, curRequestTime, 0, newKey.getValue(), newKey.getRef());
            datum.setLastList(true);
            datum.setSkipped(skipping);
            collector.add(datum.getTuple());
        }
    }
    
    /**
     * Time to move the request time forward. We take some percentage of the remaining range (since we have
     * no way of knowing how many FetchSetDatums we'll be generating), pick a random number in
     * this range, add to our current request time, and use that as the new request time.
     * 
     * @param rand
     * @param divisor What slice of remaining time range to consume (randomly)
     * @param curRequestTime Current time (actually offset)
     * @return new request time that has been randomly moved forward.
     */
    public static long randRequestTime(Random rand, long divisor, long curRequestTime) {
        
        // We want to advance by some amount
        long targetRange = (Math.max(1, Long.MAX_VALUE - curRequestTime) / divisor) - 1;
        return curRequestTime + 1 + (Math.abs(rand.nextLong()) % targetRange);
    }

}
