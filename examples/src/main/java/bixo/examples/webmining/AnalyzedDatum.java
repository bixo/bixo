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
package bixo.examples.webmining;

import com.scaleunlimited.cascading.BaseDatum;

import bixo.datum.Outlink;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


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

    public AnalyzedDatum(Fields fields, String url, float pageScore, PageResult[] pageResults, Outlink[] outlinks) {
        super(fields);
        
        setUrl(url);
        setPageScore(pageScore);
        setPageResults(pageResults);
        setOutlinks(outlinks);
    }

    public AnalyzedDatum(String url, float pageScore, PageResult[] pageResults, Outlink[] outlinks) {
        this(FIELDS, url, pageScore, pageResults, outlinks);
    }
    
    public void setUrl(String url) {
        _tupleEntry.setString(URL_FIELD, url);
    }
    
    public String getUrl() {
         return _tupleEntry.getString(URL_FIELD);
    }

    public void setPageScore(float pageScore) {
        _tupleEntry.setFloat(PAGE_SCORE_FIELD, pageScore);
    }

    public float getPageScore() {
        return _tupleEntry.getFloat(PAGE_SCORE_FIELD);
    }

    public void setPageResults(PageResult[] results) {
        _tupleEntry.setObject(PAGE_RESULTS_FIELD, makeTupleOfPageResults(results));
    }

    public PageResult[] getPageResults() {
        return makePageResultsFromTuple((Tuple)_tupleEntry.getObject(PAGE_RESULTS_FIELD));
    }

    public void setOutlinks(Outlink[] outlinks) {
        _tupleEntry.setObject(OUTLINKS_FIELD, makeTupleOfOutlinks(outlinks));
    }


    public Outlink[] getOutlinks() {
        return makeOutlinksFromTuple((Tuple)_tupleEntry.getObject(OUTLINKS_FIELD));
    }

    private Tuple makeTupleOfPageResults(PageResult[] results) {
        Tuple t = new Tuple();
        for (PageResult pr : results) {
            t.add(pr);
        }
        
        return t;
    }

    private PageResult[] makePageResultsFromTuple(Tuple tuple) {
        PageResult[] result = new PageResult[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            result[i] = (PageResult)tuple.getObject(i);
        }
        
        return result;
    }

    private Tuple makeTupleOfOutlinks(Outlink[] outlinks) {
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
