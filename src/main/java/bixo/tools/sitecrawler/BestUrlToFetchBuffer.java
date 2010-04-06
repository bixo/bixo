package bixo.tools.sitecrawler;

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
public class BestUrlToFetchBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(BestUrlToFetchBuffer.class);
    
    private long _numSelected = 0;
    
    public BestUrlToFetchBuffer() {
        super(UrlDatum.FIELDS.append(MetaData.FIELDS));
    }
    
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting selection of best URLs to fetch");
        
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending selection of best URLs to fetch - selected " + _numSelected + " urls");
    }

    @Override
    public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
        UrlDatum bestDatum = null;
        
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            UrlDatum datum = new UrlDatum(iter.next().getTuple(), MetaData.FIELDS);
//            LOGGER.info("Processing url : " + datum.getUrl());
            if (bestDatum == null) {
                bestDatum = datum;
            } else if (datum.getLastFetched() > bestDatum.getLastFetched()) {
                bestDatum = datum;
            }    
        }
        
        
        if (bestDatum != null && bestDatum.getLastFetched() == 0) {
            bufferCall.getOutputCollector().add(bestDatum.toTuple());
            _numSelected++;
        }
    }

}
