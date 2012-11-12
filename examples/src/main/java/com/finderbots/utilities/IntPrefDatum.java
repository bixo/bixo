package com.finderbots.utilities;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.bixolabs.cascading.BaseDatum;

/**
 * Created with IntelliJ IDEA.
 * User: pat
 * Date: 11/11/12
 * Time: 5:18 PM
 * To change this template use File | Settings | File Templates.
 */

public class IntPrefDatum extends BaseDatum {

    public static final String INT_PERSON_ID_FIELD = fieldName(TextPrefDatum.class, "intpersonid");
    public static final String INT_PREFERENCE_ID_FIELD = fieldName(TextPrefDatum.class, "intpreferenceid");


    public static final Fields FIELDS = new Fields( INT_PERSON_ID_FIELD,
            INT_PREFERENCE_ID_FIELD);

    public IntPrefDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public void setIntPersonIdField(String textPersonIdField) {
        _tupleEntry.set(INT_PERSON_ID_FIELD, textPersonIdField);
    }

    public int getIntPersonIdField() {
        return _tupleEntry.getInteger(INT_PERSON_ID_FIELD);
    }

    public void setIntPreferenceIdField(int textPreferenceIdField) {
        _tupleEntry.set(INT_PREFERENCE_ID_FIELD, textPreferenceIdField);
    }

    public int getPageScore() {
        return _tupleEntry.getInteger(INT_PREFERENCE_ID_FIELD);
    }

}
