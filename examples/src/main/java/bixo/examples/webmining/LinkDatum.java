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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;


@SuppressWarnings("serial")
public class LinkDatum  extends BaseDatum {

    // Cascading field names that correspond to the datum fields.
    public static final String URL_FN = fieldName(LinkDatum.class, "url");
    public static final String PAGE_SCORE_FN = fieldName(LinkDatum.class, "pagescore");
    public static final String LINK_SCORE_FN = fieldName(LinkDatum.class, "linkscore");
    public static final Fields FIELDS = new Fields(URL_FN, PAGE_SCORE_FN, LINK_SCORE_FN);

   public LinkDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public LinkDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }

    public LinkDatum(Fields fields, String url, float pageScore, float linkScore) {
        super(fields);
        
        setUrl(url);
        setPageScore(pageScore);
        setLinkScore(linkScore);
    }

    public LinkDatum(String url, float pageScore, float linkScore) {
        this(FIELDS, url, pageScore, linkScore);
    }
    
    public void setUrl(String url) {
        _tupleEntry.setString(URL_FN, url);
    }
    
    public String getUrl() {
         return _tupleEntry.getString(URL_FN);
    }

    public void setLinkScore(float linkScore) {
        _tupleEntry.setFloat(LINK_SCORE_FN, linkScore);
    }
    
    public float getLinkScore() {
        return _tupleEntry.getFloat(LINK_SCORE_FN);
    }

    public void setPageScore(float pageScore) {
        _tupleEntry.setFloat(PAGE_SCORE_FN, pageScore);
    }

    public float getPageScore() {
        return _tupleEntry.getFloat(PAGE_SCORE_FN);
    }

    public String toString() {
        return getUrl() + "\t" + getPageScore() + "\t" + getLinkScore();
    }

}
