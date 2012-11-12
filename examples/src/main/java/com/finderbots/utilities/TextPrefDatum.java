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
package com.finderbots.utilities;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.bixolabs.cascading.BaseDatum;

@SuppressWarnings("serial")
public class TextPrefDatum extends BaseDatum {

    public static final String TEXT_PERSON_ID_FIELD = fieldName(TextPrefDatum.class, "textpersonid");
    public static final String TEXT_PREFERENCE_ID_FIELD = fieldName(TextPrefDatum.class, "textpreferenceid");


    public static final Fields FIELDS = new Fields( TEXT_PERSON_ID_FIELD,
                                                    TEXT_PREFERENCE_ID_FIELD);

    public TextPrefDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public TextPrefDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }

    public void setTextPersonIdField(String textPersonIdField) {
        _tupleEntry.set(TEXT_PERSON_ID_FIELD, textPersonIdField);
    }

    public String getTextPersonIdField() {
         return _tupleEntry.getString(TEXT_PERSON_ID_FIELD);
    }

    public void setTextPreferenceIdField(float textPreferenceIdField) {
        _tupleEntry.set(TEXT_PREFERENCE_ID_FIELD, textPreferenceIdField);
    }

    public String getPageScore() {
        return _tupleEntry.getString(TEXT_PREFERENCE_ID_FIELD);
    }

}

