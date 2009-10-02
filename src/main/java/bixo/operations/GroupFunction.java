package bixo.operations;

import bixo.datum.GroupedUrlDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.util.IGroupingKeyGenerator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings({ "serial", "unchecked" })
public class GroupFunction extends BaseOperation implements Function {

    private final IGroupingKeyGenerator _generator;
    private final Fields _metaDataFieldNames;

    public GroupFunction(Fields metaDataFieldNames, IGroupingKeyGenerator generator) {
        super(new Fields(GroupedUrlDatum.GROUP_KEY_FIELD));
        
        _metaDataFieldNames = metaDataFieldNames;
        _generator = generator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall funCall) {
        String key = _generator.getGroupingKey(new UrlDatum(funCall.getArguments().getTuple(), _metaDataFieldNames));
        funCall.getOutputCollector().add(new Tuple(key));
    }

}
