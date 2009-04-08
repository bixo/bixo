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

import org.apache.hadoop.io.BytesWritable;

import bixo.Constants;
import bixo.fetcher.beans.FetchContent;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FetchedContentTuple {

    private static Fields FIELDS = new Fields(Constants.URL, Constants.CONTENT);
    private TupleEntry _tupleEntry;

    public FetchedContentTuple() {
        _tupleEntry = new TupleEntry();
    }

    public FetchedContentTuple(String url, FetchContent content) {
        byte[] bytes = content.getContent();
        if (bytes == null) {
            bytes = new byte[0];
        }
        _tupleEntry = new TupleEntry(FIELDS, new Tuple(url, new BytesWritable()));
    }

    public Tuple toTuple() {
        return _tupleEntry.getTuple();
    }

}
