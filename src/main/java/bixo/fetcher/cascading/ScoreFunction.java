package bixo.fetcher.cascading;

import java.io.IOException;

import bixo.fetcher.util.ScoreGenerator;
import bixo.tuple.UrlTuple;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;

public class ScoreFunction extends BaseOperation<Double> implements Function<Double> {

    private final ScoreGenerator _scoreGenerator;

    public ScoreFunction(ScoreGenerator scoreGenerator) {
        _scoreGenerator = scoreGenerator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<Double> funCall) {
        double generatedScore;
        try {
            generatedScore = _scoreGenerator.generateScore(new UrlTuple(funCall.getArguments().getTuple()));
            funCall.getOutputCollector().add(new Tuple(generatedScore));
        } catch (IOException e) {
            // we throw the exception here to get this data into the trap
            throw new RuntimeException("Unable to generate score for: " + funCall.getArguments(), e);
        }
    }
}
