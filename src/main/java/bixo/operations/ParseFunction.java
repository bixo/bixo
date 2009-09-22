package bixo.operations;

import bixo.cascading.NullContext;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.parser.IParser;
import bixo.parser.ParserCounters;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ParseFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private IParser _parser;
    private Fields _metaDataFields;

    public ParseFunction(IParser parser, Fields metaDataFields) {
        super(ParsedDatum.FIELDS.append(metaDataFields));
        _metaDataFields = metaDataFields;
        _parser = parser;
    }

    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        FetchedDatum fetchedDatum = new FetchedDatum(arguments.getTuple(), _metaDataFields);
        ParsedDatum parseResult = _parser.parse(fetchedDatum);
        
        // TODO KKr - add status to ParsedDatum, use it here to increment parsed vs. failed doc counters.
        // Or since this operation is part of a regular Cascading flow, we could trap exceptions.
        if (parseResult != null) {
            flowProcess.increment(ParserCounters.DOCUMENTS_PARSED, 1);
            functionCall.getOutputCollector().add(parseResult.toTuple());
        }
    }
}
