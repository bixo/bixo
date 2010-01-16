package bixo.fetcher.util;

@SuppressWarnings("serial")
public class FixedScoreGenerator extends ScoreGenerator {

    public static final double DEFAULT_SCORE = 1.0;
    
    private double _score;
    
    public FixedScoreGenerator() {
        this(DEFAULT_SCORE);
    }
    
    public FixedScoreGenerator(double score) {
        super();
        
        _score = score;
    }
    
    @Override
    public double generateScore(String domain, String pld, String url) {
        return _score;
    }
}
