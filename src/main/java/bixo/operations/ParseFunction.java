package bixo.operations;

import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.parser.IParser;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ParseFunction extends BaseOperation<ParsedDatum> implements Function<ParsedDatum> {

    private IParser _parser;
    private Fields _metaDataFields;

    public ParseFunction(Fields parsedFields, Fields metaDataFields, IParser parser) {
        super(parsedFields.append(metaDataFields));
        _metaDataFields = metaDataFields;
        _parser = parser;
    }

    public void operate(FlowProcess flowProcess, FunctionCall<ParsedDatum> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        FetchedDatum fetchedDatum = new FetchedDatum(arguments.getTuple(), _metaDataFields);
        ParsedDatum parseResult = _parser.parse(fetchedDatum);
        functionCall.getOutputCollector().add(parseResult.toTuple());
    }
}
