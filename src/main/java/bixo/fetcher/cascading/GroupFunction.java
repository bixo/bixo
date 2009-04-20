package bixo.fetcher.cascading;

import java.io.IOException;

import bixo.IConstants;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.tuple.UrlTuple;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class GroupFunction extends BaseOperation<String> implements Function<String> {

    private final IGroupingKeyGenerator _generator;

    public GroupFunction(String fieldName, IGroupingKeyGenerator generator) {
        // TODO SGr - shouldn't <fieldName> be used to call new Fields(), vs. GROUPING_KEY?
        super(new Fields(IConstants.GROUPING_KEY));
        _generator = generator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<String> funCall) {
        try {
            String key = _generator.getGroupingKey(new UrlTuple(funCall.getArguments().getTuple()));
            funCall.getOutputCollector().add(new Tuple(key));
        } catch (IOException e) {
            // we throw the exception here to get this data into the trap
            throw new RuntimeException("Unable to generate grouping key for: " + funCall.getArguments(), e);
        }
    }

}
