package bixo.examples;

import java.util.Iterator;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class LatestUrlDatumBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(LatestUrlDatumBuffer.class);

    private int _numIgnored;
    private int _numLater;

    public LatestUrlDatumBuffer() {
        super(UrlDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting selection of outlink URLs");

        _numIgnored = 0;
        _numLater = 0;
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending selection of outlink URLs");
        LOGGER.info(String.format("Ignored a total of %d duplicate URL(s) with earlier (or no) fetch time", _numIgnored));
        if (_numLater > 0) {
            LOGGER.info(String.format("Picked a total of %d URL(s) with later fetch time", _numLater));
        }
    }

    @Override
    public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
        UrlDatum bestDatum = null;

        int ignoredUrls = 0;
        long bestFetched = 0;
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            UrlDatum datum = new UrlDatum(iter.next());
            if (bestDatum == null) {
                bestDatum = datum;
                bestFetched = (Long) bestDatum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
            } else if ((Long) datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD) > bestFetched) {
                if (bestFetched != 0) {
                    _numLater += 1;
                    // Should never happen that we double-fetch a page
                    LOGGER.warn("Using URL with later fetch time: " + datum.getUrl());
                }

                // last fetched time will be 0 for never-fetched
                bestDatum = datum;
                bestFetched = (Long) bestDatum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
            } else {
                ignoredUrls += 1;
            }
        }

        _numIgnored += ignoredUrls;
        if (ignoredUrls >= 100) {
            LOGGER.info(String.format("Ignored %d duplicate URL(s) with earlier (or no) fetch time: %s", ignoredUrls, bestDatum.getUrl()));
        }

        if (bestDatum != null) {
            bufferCall.getOutputCollector().add(bestDatum.getTuple());
        }
    }

}
