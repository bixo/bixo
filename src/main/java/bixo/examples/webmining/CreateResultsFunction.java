/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class CreateResultsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateResultsFunction.class);

    public CreateResultsFunction() {
        super(new Fields("line"));
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of PageResults output");
        super.prepare(process, operationCall);
        
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        
        AnalyzedDatum datum = new AnalyzedDatum(funcCall.getArguments().getTuple());

        TupleEntryCollector collector = funcCall.getOutputCollector();
        PageResult[] pageResults = datum.getPageResults();
        if (pageResults.length > 0) {
            for (PageResult pageResult : pageResults) {
                String outResult = String.format("%s\t%s\t%s", pageResult.getSourceUrl(), pageResult.getImageUrl(), pageResult.getDescription());
                collector.add(new Tuple(outResult));
            }
        }
    }
    
}
