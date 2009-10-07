package bixo.fetcher.util;

import bixo.datum.GroupedUrlDatum;

@SuppressWarnings("serial")
public class SimpleScoreGenerator implements IScoreGenerator {
	public static final double DEFAULT_SCORE = 1.0;
	
	private final double _score;
	
	public SimpleScoreGenerator() {
		this(DEFAULT_SCORE);
	}
	
	public SimpleScoreGenerator(double score) {
		_score = score;
	}
	
	@Override
	public double generateScore(GroupedUrlDatum urlTuple) {
		return _score;
	}
}
