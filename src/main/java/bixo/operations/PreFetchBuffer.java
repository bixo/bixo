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
import bixo.fetcher.http.IRobotRules;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings( { "serial", "unchecked" })
public class PreFetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(PreFetchBuffer.class);
    
    private FetcherPolicy _fetcherPolicy;
    private int _numReduceTasks;
    private final Fields _metaDataFields;

    public PreFetchBuffer(FetcherPolicy fetcherPolicy, int numReduceTasks, Fields metaDataFields) {
        super(PreFetchedDatum.FIELDS.append(metaDataFields));

        _fetcherPolicy = fetcherPolicy;
        _numReduceTasks = numReduceTasks;
        _metaDataFields = metaDataFields;
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
        if (crawlDelay == IRobotRules.UNSET_CRAWL_DELAY) {
            crawlDelay = _fetcherPolicy.getCrawlDelay();
        }
        
        TupleEntryCollector collector = buffCall.getOutputCollector();

        PartitioningKey newKey = new PartitioningKey(key, _numReduceTasks);
        long curRequestTime = System.currentTimeMillis();
        long nextRequestTime = curRequestTime;

        int targetSize = 0;

        List<ScoredUrlDatum> urls = new LinkedList<ScoredUrlDatum>();
        while (values.hasNext()) {
            if (targetSize == 0) {
                // Figure out the max # of URLs that we would want to get.
                FetchRequest request = _fetcherPolicy.getFetchRequest(curRequestTime, crawlDelay, Integer.MAX_VALUE);
                targetSize = request.getNumUrls();
                nextRequestTime = request.getNextRequestTime();
            }

            ScoredUrlDatum scoredDatum = new ScoredUrlDatum(values.next().getTuple(), _metaDataFields);
            urls.add(scoredDatum);

            if (urls.size() >= targetSize) {
                LOGGER.debug(String.format("Added %d urls for key %s at %d", urls.size(), key, curRequestTime));
                PreFetchedDatum datum = new PreFetchedDatum(urls, curRequestTime, nextRequestTime - curRequestTime, newKey, !values.hasNext());
                collector.add(datum.toTuple());

                curRequestTime = nextRequestTime;
                urls.clear();
                targetSize = 0;
            }
        }
        
        // See if we have another partially built datum to add.
        if (urls.size() > 0) {
            LOGGER.debug(String.format("Added %d urls for key %s at %d", urls.size(), key, curRequestTime));
            PreFetchedDatum datum = new PreFetchedDatum(urls, curRequestTime, 0, newKey, true);
            collector.add(datum.toTuple());
        }
    }

}
