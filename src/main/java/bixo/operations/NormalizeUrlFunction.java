package bixo.operations;

import bixo.cascading.NullContext;
import bixo.datum.UrlDatum;
import bixo.urldb.IUrlNormalizer;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class NormalizeUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final IUrlNormalizer _normalizer;
    private final Fields _metaDataFieldNames;

    public NormalizeUrlFunction(IUrlNormalizer normalizer, Fields metaDataFieldNames) {
        super(UrlDatum.FIELDS);
        
        _normalizer = normalizer;
        _metaDataFieldNames = metaDataFieldNames;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        UrlDatum datum = new UrlDatum(funCall.getArguments().getTuple(), _metaDataFieldNames);
        datum.setUrl(_normalizer.normalize(datum.getUrl()));
        funCall.getOutputCollector().add(datum.toTuple());
    }
}
