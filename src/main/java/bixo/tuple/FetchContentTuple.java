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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FetchContentTuple extends BaseTuple {

    private static Fields FIELDS = new Fields(Constants.BASE_URL, Constants.FETECHED_URL, Constants.FETCH_TIME, Constants.CONTENT, Constants.CONTENT_TYPE);

    public FetchContentTuple(String baseUrl, String fetchedUrl, long fetchTime, byte[] content, String contentType) {
        super(new TupleEntry(FIELDS, Tuple.size(FIELDS.size())));
        setBaseUrl(baseUrl);
        setFetchedUrl(fetchedUrl);
        setFetchTime(fetchTime);
        setContent(content);
        setContentType(contentType);
    }

    public FetchContentTuple(Tuple tuple) {
        super(new TupleEntry(FIELDS, tuple));
    }

    public String getBaseUrl() {
        return getTupleEntry().getString(Constants.BASE_URL);
    }

    public void setBaseUrl(String baseUrl) {
        getTupleEntry().set(Constants.BASE_URL, baseUrl);
    }

    public String getFetchedUrl() {
        return getTupleEntry().getString(Constants.FETECHED_URL);
    }

    public void setFetchedUrl(String fetchedUrl) {
        getTupleEntry().set(Constants.FETECHED_URL, fetchedUrl);
    }

    public long getFetchTime() {
        return getTupleEntry().getLong(Constants.FETCH_TIME);
    }

    public void setFetchTime(long fetchTime) {
        getTupleEntry().set(Constants.FETCH_TIME, fetchTime);
    }

    public byte[] getContent() {
        BytesWritable content = (BytesWritable) getTupleEntry().get(Constants.CONTENT);
        return content.getBytes();
    }

    public void setContent(byte[] content) {
        if (content == null) {
            content = new byte[0];
        }
        getTupleEntry().set(Constants.CONTENT, new BytesWritable(content));
    }

    public String getContentType() {
        return getTupleEntry().getString(Constants.CONTENT_TYPE);
    }

    public void setContentType(String contentType) {
        getTupleEntry().set(Constants.CONTENT_TYPE, contentType);
    }

}
