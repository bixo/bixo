package bixo.parser;

import bixo.tuple.FetchContentTuple;
import bixo.tuple.ParseResultTuple;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ParseFunction extends BaseOperation<ParseResultTuple> implements Function<ParseResultTuple> {

    private IParserFactory _factory;

    public ParseFunction(IParserFactory factory) {
        super(ParseResultTuple.FIELDS);
        _factory = factory;
    }

    public void operate(FlowProcess flowProcess, FunctionCall<ParseResultTuple> functionCall) {
        IParser parser = _factory.newParser();
        TupleEntry arguments = functionCall.getArguments();
        FetchContentTuple contentTuple = new FetchContentTuple(arguments.getTuple());
        ParseResultTuple parseResult = parser.parse(contentTuple);
        functionCall.getOutputCollector().add(parseResult.toTuple());
    }
}
