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
import bixo.IConstants;
import bixo.fetcher.beans.FetchStatusCode;

public class FetchResultTuple extends BaseTuple {
    public static final Fields FIELDS = new Fields( IConstants.FETCH_STATUS, IConstants.FETCH_CONTENT);

    public FetchResultTuple(FetchStatusCode statusCode, FetchContentTuple content) {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
//        setUrl(url);
        setStatusCode(statusCode);
        setFetchContent(content);
    }

    public FetchResultTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

//    public String getUrl() {
//        return getTupleEntry().getString(Constants.URL);
//    }
//
//    public void setUrl(String fetchedUrl) {
//        getTupleEntry().set(Constants.URL, fetchedUrl);
//    }

    public void setFetchContent(FetchContentTuple content) {
        getTupleEntry().set(IConstants.FETCH_CONTENT, content.toTuple());
    }

    public FetchContentTuple getContent() {
        return new FetchContentTuple((Tuple) getTupleEntry().get(IConstants.FETCH_CONTENT));
    }

    public void setStatusCode(FetchStatusCode statusCode) {
        getTupleEntry().set(IConstants.FETCH_STATUS, statusCode.ordinal());
    }

    public FetchStatusCode getStatusCode() {
        return FetchStatusCode.fromOrdinal(getTupleEntry().getInteger(IConstants.FETCH_STATUS));
    }

    public String toString() {
        int size = getContent().getContent() == null ? 0 : getContent().getContent().length;
        return String.format("%s (status code %s, size %d)", getContent().getFetchedUrl(), getStatusCode().toString(), size);
    }
}
