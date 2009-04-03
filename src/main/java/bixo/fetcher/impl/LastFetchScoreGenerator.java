package bixo.fetcher.impl;

import bixo.fetcher.ScoreGenerator;
import bixo.items.UrlItem;

public class LastFetchScoreGenerator implements ScoreGenerator {

    private final long _now;
    private final long _interval;

    public LastFetchScoreGenerator(long now, long interval) {
        _now = now;
        _interval = interval;
    }

    @Override
    public double generateScore(UrlItem urlItem) {
        long lastFetched = urlItem.getLastFetched();
        if (lastFetched == 0) {
            return 1d;
        }
        long offset = _now - lastFetched;
        if (offset >= _interval) {
            return 1d;
        }
        return (double) offset / _interval;
    }
}
