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
package bixo.tuple;

import bixo.Constants;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FetchTuple implements Comparable<FetchTuple> {

    private static Fields FIELDS = new Fields(Constants.URL, Constants.SCORE);
    private TupleEntry _tupleEntry;

    public FetchTuple(String url, double score) {
        _tupleEntry = new TupleEntry(FIELDS, new Tuple(url, score));
    }

    public FetchTuple(Tuple tuple) {
        _tupleEntry = new TupleEntry(FIELDS, tuple);
    }

    public String getUrl() {
        return _tupleEntry.getString(Constants.URL);
    }

    public double getScore() {
        return _tupleEntry.getDouble(Constants.SCORE);
    }

    public Tuple toTuple() {
        return _tupleEntry.getTuple();
    }

    @Override
    public int compareTo(FetchTuple o) {
        // Sort in reverse order, such that higher scores are first.
        if (getScore()> o.getScore()) {
            return -1;
        } else if (getScore()< o.getScore()) {
            return 1;
        } else {
            // TODO KKr - sort by URL, so that if we do a batch fetch, we're
            // fetching pages from the same area of the website.
            
            // TODO SG adding a simple sting comparison for now. 
            return getUrl().compareTo(o.getUrl());
        }
    }

}
