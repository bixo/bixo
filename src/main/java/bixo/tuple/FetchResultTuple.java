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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import bixo.Constants;
import bixo.fetcher.beans.FetchStatusCode;

public class FetchResultTuple extends BaseTuple {
    private static final Fields FIELDS = new Fields(Constants.FETCH_STATUS, Constants.FETCH_CONTENT);

    public FetchResultTuple(FetchStatusCode statusCode, FetchContentTuple content) {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
        setStatusCode(statusCode);
        setFetchContent(content);
    }

    public void setFetchContent(FetchContentTuple content) {
        getTupleEntry().set(Constants.FETCH_CONTENT, content.toTuple());
    }

    public FetchContentTuple getContent() {
        return new FetchContentTuple((Tuple) getTupleEntry().get(Constants.FETCH_CONTENT));
    }

    public void setStatusCode(FetchStatusCode statusCode) {
        getTupleEntry().set(Constants.FETCH_STATUS, statusCode.ordinal());
    }

    public FetchStatusCode getStatusCode() {
        return FetchStatusCode.fromOrdinal(getTupleEntry().getInteger(Constants.FETCH_STATUS));
    }

    public String toString() {
        int size = getContent().getContent() == null ? 0 : getContent().getContent().length;
        return String.format("%s (status code %s, size %d)", getContent().getFetchedUrl(), getStatusCode().toString(), size);
    }
}
