package bixo.operations;

import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.cascading.PartitioningKey;
import bixo.config.BaseFetchJobPolicy;
import bixo.config.BaseFetchJobPolicy.FetchSetInfo;
import bixo.datum.FetchSetDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
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

    private int _numReduceTasks;
    private BaseFetchJobPolicy _policy;
    
    private boolean _iteratorDone;
    private Iterator<TupleEntry> _values;

    public MakeFetchSetsBuffer(BaseFetchJobPolicy policy, int numReduceTasks) {
        super(FetchSetDatum.FIELDS);

        _policy = policy;
        _numReduceTasks = numReduceTasks;
    }

    @Override
    public void operate(FlowProcess process, BufferCall buffCall) {
        Iterator<TupleEntry> values = buffCall.getArgumentsIterator();
        TupleEntry group = buffCall.getGroup();
        
        _values = values;
        _iteratorDone = false;

        // <key> is the output of the IGroupingKeyGenerator used. This should
        // be <IP address>-<crawl delay in ms>
        String key = group.getString(0);

        if (GroupingKey.isSpecialKey(key)) {
            throw new RuntimeException("Invalid grouping key: " + key);
        }

        long crawlDelay = GroupingKey.getCrawlDelayFromKey(key);
        if (crawlDelay == BaseFetchJobPolicy.UNSET_CRAWL_DELAY) {
            crawlDelay = _policy.getDefaultCrawlDelay();
        }
        
        _policy.startFetchSet(key, crawlDelay);
        
        TupleEntryCollector collector = buffCall.getOutputCollector();

        PartitioningKey newKey = new PartitioningKey(key, _numReduceTasks);
        
        while (safeHasNext()) {
            ScoredUrlDatum scoredDatum = new ScoredUrlDatum(new TupleEntry(values.next()));
            FetchSetInfo setInfo = _policy.nextFetchSet(scoredDatum);
            if (setInfo != null) {
                FetchSetDatum result = makeFetchSetDatum(setInfo, newKey, safeHasNext());
                collector.add(result.getTuple());
            }
        }
        
        // See if we have another partially built datum to add.
        FetchSetInfo setInfo = _policy.endFetchSet();
        if (setInfo != null) {
            FetchSetDatum result = makeFetchSetDatum(setInfo, newKey, false);
            collector.add(result.getTuple());
        }
    }

    private FetchSetDatum makeFetchSetDatum(FetchSetInfo setInfo, PartitioningKey key, boolean hasNext) {
        LOGGER.trace(String.format("Added %d urls for ref %s in group %d at %d", setInfo.getUrls().size(), key.getRef(), key.getValue(), setInfo.getSortKey()));
        
        FetchSetDatum result = new FetchSetDatum(setInfo.getUrls(), setInfo.getSortKey(), setInfo.getFetchDelay(), key.getValue(), key.getRef());
        result.setLastList(!hasNext || setInfo.isSkipping());
        result.setSkipped(setInfo.isSkipping());
        return result;
    }
    
    
    /**
     * Return true if the iterator has another Tuple. This avoids calling
     * the hasNext() method after it returns false, as doing so with
     * Cascading 1.2 will trigger a NPE.
     * 
     * @return true if there's another Tuple waiting to be read.
     */
    private boolean safeHasNext() {
        _iteratorDone = _iteratorDone || !_values.hasNext();
        return !_iteratorDone;
    }
    

}
