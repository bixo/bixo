package bixo.operations;

import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlNormalizer;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class NormalizeUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final BaseUrlNormalizer _normalizer;

    public NormalizeUrlFunction(BaseUrlNormalizer normalizer) {
        super(UrlDatum.FIELDS);
        
        _normalizer = normalizer;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        UrlDatum datum = new UrlDatum(funCall.getArguments());
        
        // Create copy, since we're setting a field, and the tuple is going to be unmodifiable.
        UrlDatum result = new UrlDatum(datum);
        result.setUrl(_normalizer.normalize(datum.getUrl()));
        funCall.getOutputCollector().add(result.getTuple());
    }
}
