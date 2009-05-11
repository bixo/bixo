package bixo.operations;

import bixo.urldb.IUrlNormalizer;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "serial", "unchecked" })
public class NormalizeFunction extends BaseOperation implements Function {

    private final IUrlNormalizer _urlNormalizer;
    private final String _inputFieldName;
    
    /**
     * Create Cascading function to create normalized URL field from input URL field
     * 
     * @param inputField - Field that contains the input URL
     * @param outputField - Field that contains the normalized URL
     * @param urlNormalizer - Class to use for normalization
     */
    public NormalizeFunction(String inputFieldName, Fields outputField, IUrlNormalizer urlNormalizer) {
        super(outputField);

        _inputFieldName = inputFieldName;
        _urlNormalizer = urlNormalizer;

        if (outputField.size() != 1) {
            throw new IllegalArgumentException("May only declare one field name, got: " + outputField.printVerbose());
        }
    }

    @Override
    public void operate(FlowProcess process, FunctionCall funCall) {
        TupleEntry entry = funCall.getArguments();
        
        String normalizedUrl = _urlNormalizer.normalize(entry.getString(_inputFieldName));
        funCall.getOutputCollector().add(new Tuple(normalizedUrl));
    }
}
