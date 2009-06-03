package bixo.operations;

import bixo.urldb.IUrlFilter;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings( { "serial", "unchecked" })
public class UrlFilterFunction extends BaseOperation implements Function {

    private final IUrlFilter _urlFilter;
    private final String _inputFieldName;

    /**
     * Create Cascading function to filter urls
     */
    public UrlFilterFunction(Fields outputFields, String inputFieldName, IUrlFilter urlFilter) {
        super(outputFields);
        _inputFieldName = inputFieldName;
        _urlFilter = urlFilter;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall funCall) {
        TupleEntry entry = funCall.getArguments();

        String filteredUrl = entry.getString(_inputFieldName);
        if (_urlFilter.filter(filteredUrl) != null) {
            funCall.getOutputCollector().add(entry);
        }
    }
}
