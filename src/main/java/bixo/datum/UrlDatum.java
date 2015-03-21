/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.datum;


import com.scaleunlimited.cascading.PayloadDatum;

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
        _tupleEntry.setString(URL_FN, url);
    }

}
