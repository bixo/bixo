package com.finderbots.miner2.tomatoes;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;
import com.bixolabs.cascading.NullContext;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: pat
 * Date: 7/3/13
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */

    @SuppressWarnings("serial")
public class FilterMediaDatumFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateRTMediaRecordsFunction.class);

    public FilterMediaDatumFunction() {
        //super(new Fields("line"));
        super (RTPageDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of PageResults output");
        super.prepare(process, operationCall);

    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {

        RTPageDatum datum = new RTPageDatum(funcCall.getArguments().getTuple());

        TupleEntryCollector collector = funcCall.getOutputCollector();
        String itemId = datum.getItemId();
        String itemName = datum.getItemName();
        String posterImageUrl = datum.getPosterImageUrl();
        if (itemId != null && itemName != null && posterImageUrl != null &&
            !itemId.isEmpty() && !itemName.isEmpty() && !posterImageUrl.isEmpty()) {
            //String outResult = String.format("%s\t%s\t%s", datum.getItemId(), datum.getItemName(), datum.getPosterImageUrl());
            collector.add(funcCall.getArguments().getTuple());
        }
    }

}