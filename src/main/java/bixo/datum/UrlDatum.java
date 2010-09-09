/*
 * Copyright (c) 2010 TransPac Software, Inc.
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
package bixo.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class UrlDatum extends PayloadDatum {
    
    public static final String URL_FN = fieldName(UrlDatum.class, "url");
    public static final Fields FIELDS = new Fields(URL_FN).append(getSuperFields(UrlDatum.class));
    
    public UrlDatum() {
        super(FIELDS);
    }

    public UrlDatum(UrlDatum datum) {
        super(new TupleEntry(datum.getTupleEntry()));
    }
    
    public UrlDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }

    public UrlDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
        validateFields(fields, FIELDS);
    }

    public UrlDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    public UrlDatum(Fields fields, String url) {
        super(fields);
        validateFields(fields, FIELDS);
        setUrl(url);
    }
    
    public UrlDatum(String url) {
        super(FIELDS);
        setUrl(url);
    }

    public String getUrl() {
        return _tupleEntry.getString(URL_FN);
    }

    public void setUrl(String url) {
        _tupleEntry.set(URL_FN, url);
    }

}
