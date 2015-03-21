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

import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ScoredUrlDatum extends GroupedUrlDatum implements Serializable {
    
    private static final String STATUS_FN = fieldName(ScoredUrlDatum.class, "status");
    private static final String SCORE_FN = fieldName(ScoredUrlDatum.class, "score");
    public static final Fields FIELDS = new Fields(STATUS_FN, SCORE_FN).append(getSuperFields(ScoredUrlDatum.class));
    
    private static final double DEFAULT_SCORE = 1.0;

    public ScoredUrlDatum() {
        super(FIELDS);
    }
    
    public ScoredUrlDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public ScoredUrlDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }

    public ScoredUrlDatum(String url) {
        this(url, "", UrlStatus.UNFETCHED);
    }
    
    public ScoredUrlDatum(String url, String groupKey, UrlStatus status) {
        this(url, groupKey, status, DEFAULT_SCORE);
    }

    public ScoredUrlDatum(String url, String groupKey, UrlStatus status, double score) {
        super(FIELDS, url, groupKey);
        
        setStatus(status);
        setScore(score);
    }

    public UrlStatus getStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(STATUS_FN));
    }

    public void setStatus(UrlStatus status) {
        _tupleEntry.setString(STATUS_FN, status.name());
    }
    
    public double getScore() {
        return _tupleEntry.getDouble(SCORE_FN);
    }

    public void setScore(double score) {
        _tupleEntry.setDouble(SCORE_FN, score);
    }

    public static Fields getSortingField() {
        return new Fields(SCORE_FN);
    }

}
