package bixo.operations;

import bixo.cascading.NullContext;
import bixo.datum.NormalizedUrlDatum;
import bixo.datum.UrlDatum;
import bixo.urldb.IUrlNormalizer;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class NormalizeFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final IUrlNormalizer _urlNormalizer;

    public NormalizeFunction(IUrlNormalizer urlNormalizer) {
        super(new Fields(NormalizedUrlDatum.NORMALIZED_URL_FIELD));
        
        _urlNormalizer = urlNormalizer;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        TupleEntry entry = funCall.getArguments();
        
        String normalizedUrl = _urlNormalizer.normalize(entry.getString(UrlDatum.URL_FIELD));
        funCall.getOutputCollector().add(new Tuple(normalizedUrl));
    }
}
