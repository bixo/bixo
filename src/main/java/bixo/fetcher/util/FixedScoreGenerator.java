package bixo.fetcher.util;

@SuppressWarnings("serial")
public class FixedScoreGenerator extends ScoreGenerator {

    private double _score;
    
    public FixedScoreGenerator(double score) {
        super();
        
        _score = score;
    }
    
    @Override
    public double generateScore(String domain, String pld, String url) {
        return _score;
    }
}
