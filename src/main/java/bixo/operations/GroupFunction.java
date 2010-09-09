package bixo.operations;

import org.apache.log4j.Logger;

import bixo.datum.GroupedUrlDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.util.GroupingKeyGenerator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

@SuppressWarnings({ "serial", "unchecked" })
public class GroupFunction extends BaseOperation implements Function {
    private static final Logger LOGGER = Logger.getLogger(GroupFunction.class);

    private final GroupingKeyGenerator _generator;

    public GroupFunction(GroupingKeyGenerator generator) {
        super(GroupedUrlDatum.FIELDS);
        
        _generator = generator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall funCall) {
        String key;
        try {
            UrlDatum datum = new UrlDatum(funCall.getArguments());
            key = _generator.getGroupingKey(datum);
            GroupedUrlDatum result = new GroupedUrlDatum(datum, key);
            funCall.getOutputCollector().add(result.getTuple());
        } catch (Exception e) {
            // TODO KKr - don't lose the tuple (skipping support)
            LOGGER.error("Unexpected exception while grouping URL (probably badly formed)", e);
        }
    }
}
