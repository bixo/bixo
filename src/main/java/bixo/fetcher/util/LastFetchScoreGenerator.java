/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.fetcher.util;

import bixo.tuple.UrlWithGroupKeyTuple;

@SuppressWarnings("serial")
public class LastFetchScoreGenerator implements ScoreGenerator {
    private final long _now;
    private final long _interval;

    public LastFetchScoreGenerator(long now, long interval) {
        _now = now;
        _interval = interval;
    }

    @Override
    public double generateScore(UrlWithGroupKeyTuple urlTuple) {
        long lastFetched = urlTuple.getLastFetched();
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
