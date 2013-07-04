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
package com.finderbots.miner2.tomatoes;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.bixolabs.cascading.BaseDatum;

@SuppressWarnings("serial")
public class RTPageDatum extends BaseDatum {

    public static final String URL_FIELD = fieldName(RTPageDatum.class, "url");
//    public static final String PAGE_SCORE_FIELD = fieldName(RTPageDatum.class, "score");
    public static final String ITEM_ID_FIELD = fieldName(RTPageDatum.class, "item_id");
    public static final String ITEM_NAME_FIELD = fieldName(RTPageDatum.class, "item_name");
    public static final String POSTER_IMAGE_URL_FIELD = fieldName(RTPageDatum.class, "poster_image_url");
    public static final String PREFS_FIELD = fieldName(RTPageDatum.class, "preferences");


    public static final Fields FIELDS = new Fields( URL_FIELD, ITEM_ID_FIELD, ITEM_NAME_FIELD, POSTER_IMAGE_URL_FIELD, PREFS_FIELD);

    public RTPageDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public RTPageDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }

    public RTPageDatum(String url, String itemId, String itemName, String posterImageURL, MultiValuePreference[] prefs) {
        super(FIELDS);

        setUrl(url);
        setPrefs(prefs);
    }

    public void setUrl(String url) {
        _tupleEntry.set(URL_FIELD, url);
    }

    public String getUrl() {
        return _tupleEntry.getString(URL_FIELD);
    }

    public void setItemId(String itemId) {
        _tupleEntry.set(ITEM_ID_FIELD, itemId);
    }

    public String getItemId() {
        return _tupleEntry.getString(ITEM_ID_FIELD);
    }

    public void setItemName(String itemName) {
        _tupleEntry.set(ITEM_NAME_FIELD, itemName);
    }

    public String getItemName() {
        return _tupleEntry.getString(ITEM_NAME_FIELD);
    }

    public void setPosterImageUrl(String url) {
        _tupleEntry.set(POSTER_IMAGE_URL_FIELD, url);
    }

    public String getPosterImageUrl() {
        return _tupleEntry.getString(POSTER_IMAGE_URL_FIELD);
    }

    public void setPrefs(MultiValuePreference[] prefs) {
        _tupleEntry.set(PREFS_FIELD, makeTupleOfPrefs(prefs));
    }

    public MultiValuePreference[] getPreferences() {
        return makePrefsFromTuple((Tuple)_tupleEntry.get(PREFS_FIELD));
    }

     private Object makeTupleOfPrefs(MultiValuePreference[] prefs) {
        Tuple t = new Tuple();
        for (MultiValuePreference pref : prefs) {
            t.add(pref);
        }

        return t;
    }

    private MultiValuePreference[] makePrefsFromTuple(Tuple tuple) {
        MultiValuePreference[] prefs = new MultiValuePreference[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            prefs[i] = (MultiValuePreference)tuple.getObject(i);
        }

        return prefs;
    }



}
