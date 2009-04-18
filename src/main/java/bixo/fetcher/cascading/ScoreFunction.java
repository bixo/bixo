package bixo.fetcher.cascading;

import java.io.IOException;

import bixo.IConstants;
import bixo.fetcher.util.IScoreGenerator;
import bixo.tuple.UrlWithGroupKeyTuple;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class ScoreFunction extends BaseOperation<Double> implements Function<Double> {

    private final IScoreGenerator _scoreGenerator;

    public ScoreFunction(IScoreGenerator scoreGenerator) {
        super(new Fields(IConstants.SCORE));
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
