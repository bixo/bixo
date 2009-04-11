package bixo.fetcher.cascading;

import java.io.IOException;

import bixo.Constants;
import bixo.fetcher.util.ScoreGenerator;
import bixo.tuple.UrlWithGroupKeyTuple;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class ScoreFunction extends BaseOperation<Double> implements Function<Double> {

    private final ScoreGenerator _scoreGenerator;

    public ScoreFunction(ScoreGenerator scoreGenerator) {
        super(new Fields(Constants.SCORE));
        _scoreGenerator = scoreGenerator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<Double> funCall) {
        double generatedScore;
        UrlWithGroupKeyTuple groupedUrl = new UrlWithGroupKeyTuple(funCall.getArguments().getTuple());

        try {
            generatedScore = _scoreGenerator.generateScore(groupedUrl);
            funCall.getOutputCollector().add(new Tuple(generatedScore));
        } catch (IOException e) {
            // we throw the exception here to get this data into the trap
            throw new RuntimeException("Unable to generate score for: " + groupedUrl, e);
        }
    }
}
