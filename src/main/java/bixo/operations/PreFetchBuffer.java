package bixo.operations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.cascading.PartitioningKey;
import bixo.config.FetcherPolicy;
import bixo.datum.PreFetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.FetchRequest;
import bixo.robots.RobotRules;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings( { "serial", "unchecked" })
public class PreFetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(PreFetchBuffer.class);

    private static final int URLS_PER_SKIPPED_BATCH = 100;
    
    private FetcherPolicy _fetcherPolicy;
    private int _numReduceTasks;

    public PreFetchBuffer(FetcherPolicy fetcherPolicy, int numReduceTasks) {
        super(PreFetchedDatum.FIELDS);

        _fetcherPolicy = fetcherPolicy;
        _numReduceTasks = numReduceTasks;
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
        if (crawlDelay == RobotRules.UNSET_CRAWL_DELAY) {
            crawlDelay = _fetcherPolicy.getCrawlDelay();
        }
        
        int maxUrls = _fetcherPolicy.getMaxUrlsPerServer();
        int totalUrls = 0;
        
        TupleEntryCollector collector = buffCall.getOutputCollector();

        PartitioningKey newKey = new PartitioningKey(key, _numReduceTasks);
        long curRequestTime = System.currentTimeMillis();
        long nextRequestTime = curRequestTime;

        int targetSize = 0;

        List<ScoredUrlDatum> urls = new LinkedList<ScoredUrlDatum>();
        
        // TODO KKr - if we have a crawl duration, use it here to figure out how many URLs we
        // could process. Read in up to that many URLs and put in a DiskQueue, so we have a count, then
        // calculate a new crawlDelay that's (remaining time)/<num urls>, and use that in our loop.
        // This will do a better job of spreading URLs out, because when we sort by target time
        // the URLs from small domains will be better mingled with URLs from big domains. And this
        // in turn will improve fetch efficiency or reduce the number of URLs we wind up skipping.
        
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
                    FetchRequest request = _fetcherPolicy.getFetchRequest(curRequestTime, crawlDelay, Integer.MAX_VALUE);
                    targetSize = Math.min(request.getNumUrls(), maxUrls - totalUrls);
                    nextRequestTime = request.getNextRequestTime();
                }
            }

            ScoredUrlDatum scoredDatum = new ScoredUrlDatum(values.next());
            urls.add(scoredDatum);
            totalUrls += 1;

            if (urls.size() >= targetSize) {
                LOGGER.trace(String.format("Added %d urls for ref %s in group %d at %d", urls.size(), newKey.getRef(), newKey.getValue(), curRequestTime));
                PreFetchedDatum datum = new PreFetchedDatum(urls, curRequestTime, nextRequestTime - curRequestTime, newKey.getValue(), newKey.getRef(), !values.hasNext());
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
            PreFetchedDatum datum = new PreFetchedDatum(urls, curRequestTime, 0, newKey.getValue(), newKey.getRef(), true);
            datum.setSkipped(skipping);
            collector.add(datum.getTuple());
        }
    }

}
