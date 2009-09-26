package bixo.fetcher.util;

import java.io.IOException;

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
	public double generateScore(GroupedUrlDatum urlTuple) throws IOException {
		return _score;
	}
}
