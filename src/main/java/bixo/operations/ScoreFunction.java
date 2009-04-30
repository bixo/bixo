package bixo.operations;

import java.io.IOException;

import bixo.cascading.NullContext;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.util.IScoreGenerator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class ScoreFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final IScoreGenerator _scoreGenerator;
    private final Fields _metaDataFieldNames;

    public ScoreFunction(IScoreGenerator scoreGenerator, Fields metaDataFieldNames) {
        super(new Fields(ScoredUrlDatum.SCORE_FIELD));
        
        _scoreGenerator = scoreGenerator;
        _metaDataFieldNames = metaDataFieldNames;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        double generatedScore;
        GroupedUrlDatum groupedUrl = new GroupedUrlDatum(funCall.getArguments().getTuple(), _metaDataFieldNames);

        try {
            generatedScore = _scoreGenerator.generateScore(groupedUrl);
            funCall.getOutputCollector().add(new Tuple(generatedScore));
        } catch (IOException e) {
            // we throw the exception here to get this data into the trap
            throw new RuntimeException("Unable to generate score for: " + groupedUrl, e);
        }
    }
}
