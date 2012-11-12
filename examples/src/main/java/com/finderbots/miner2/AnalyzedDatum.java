/*
 * Copyright 2009-2012 Scale Unlimited
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
package com.finderbots.miner2;

import bixo.datum.Outlink;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.bixolabs.cascading.BaseDatum;

@SuppressWarnings("serial")
public class AnalyzedDatum extends BaseDatum {

    public static final String URL_FIELD = fieldName(AnalyzedDatum.class, "url");
    public static final String PAGE_SCORE_FIELD = fieldName(AnalyzedDatum.class, "score");
    public static final String PAGE_RESULTS_FIELD = fieldName(AnalyzedDatum.class, "pageresults");
    public static final String OUTLINKS_FIELD = fieldName(AnalyzedDatum.class, "outlinks");

    
    public static final Fields FIELDS = new Fields( URL_FIELD,
                                                    PAGE_SCORE_FIELD, 
                                                    PAGE_RESULTS_FIELD, 
                                                    OUTLINKS_FIELD);

    public AnalyzedDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public AnalyzedDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }

    public AnalyzedDatum(Fields fields, String url, float pageScore, BooleanPreference[] pageResults, Outlink[] outlinks) {
        super(fields);
        
        setUrl(url);
        setPageScore(pageScore);
        setPageResults(pageResults);
        setOutlinks(outlinks);
    }

    public AnalyzedDatum(String url, float pageScore, BooleanPreference[] pageResults, Outlink[] outlinks) {
        this(FIELDS, url, pageScore, pageResults, outlinks);
    }
    
    public void setUrl(String url) {
        _tupleEntry.set(URL_FIELD, url);
    }
    
    public String getUrl() {
         return _tupleEntry.getString(URL_FIELD);
    }

    public void setPageScore(float pageScore) {
        _tupleEntry.set(PAGE_SCORE_FIELD, pageScore);
    }

    public float getPageScore() {
        return _tupleEntry.getFloat(PAGE_SCORE_FIELD);
    }

    public void setPageResults(BooleanPreference[] results) {
        _tupleEntry.set(PAGE_RESULTS_FIELD, makeTupleOfPageResults(results));
    }

    public BooleanPreference[] getPageResults() {
        return makePageResultsFromTuple((Tuple)_tupleEntry.get(PAGE_RESULTS_FIELD));
    }

    public void setOutlinks(Outlink[] outlinks) {
        _tupleEntry.set(OUTLINKS_FIELD, makeTupleOfOutlinks(outlinks));
    }


    public Outlink[] getOutlinks() {
        return makeOutlinksFromTuple((Tuple)_tupleEntry.get(OUTLINKS_FIELD));
    }

    private Object makeTupleOfPageResults(BooleanPreference[] results) {
        Tuple t = new Tuple();
        for (BooleanPreference pr : results) {
            t.add(pr);
        }
        
        return t;
    }

    private BooleanPreference[] makePageResultsFromTuple(Tuple tuple) {
        BooleanPreference[] result = new BooleanPreference[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            result[i] = (BooleanPreference)tuple.getObject(i);
        }
        
        return result;
    }

    private Object makeTupleOfOutlinks(Outlink[] outlinks) {
        Tuple t = new Tuple();
        for (Outlink outlink : outlinks) {
            t.add(outlink);
        }
        
        return t;
    }

    private Outlink[] makeOutlinksFromTuple(Tuple tuple) {
        Outlink[] result = new Outlink[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            result[i] = (Outlink)tuple.getObject(i);
        }
        
        return result;
    }



}
