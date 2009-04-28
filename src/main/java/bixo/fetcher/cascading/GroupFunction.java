package bixo.fetcher.cascading;

import java.io.IOException;

import bixo.cascading.NullContext;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.tuple.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class GroupFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final IGroupingKeyGenerator _generator;
    private final Fields _metaDataFieldNames;

    public GroupFunction(String fieldName, Fields metaDataFieldNames, IGroupingKeyGenerator generator) {
        super(new Fields(fieldName));
        _metaDataFieldNames = metaDataFieldNames;
        _generator = generator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        try {
            String key = _generator.getGroupingKey(UrlDatum.fromTuple(funCall.getArguments().getTuple(), _metaDataFieldNames));
            funCall.getOutputCollector().add(new Tuple(key));
        } catch (IOException e) {
            // we throw the exception here to get this data into the trap
            throw new RuntimeException("Unable to generate grouping key for: " + funCall.getArguments(), e);
        }
    }

}
