package com.finderbots.utilities;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.bixolabs.cascading.BaseDatum;

/**
 * Created with IntelliJ IDEA.
 * User: pat
 * Date: 11/11/12
 * Time: 5:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class IndexDatum extends BaseDatum {

    public static final String TEXT_PERSON_ID_FIELD = fieldName(IndexDatum.class, "textpersonid");
    public static final String HASHED_PERSON_ID_FIELD = fieldName(IndexDatum.class, "hasedpersonid");


    public static final Fields FIELDS = new Fields( HASHED_PERSON_ID_FIELD, TEXT_PERSON_ID_FIELD);

    public IndexDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public IndexDatum( int hash, String name ){
        setHashedPersonIdField(hash);
        setTextPersonIdField(name);
    }

    public void setTextPersonIdField(String textPersonId) {
        _tupleEntry.set(TEXT_PERSON_ID_FIELD, textPersonId);
    }

    public String getTextPersonIdField() {
        return _tupleEntry.getString(TEXT_PERSON_ID_FIELD);
    }
    public void setHashedPersonIdField(int hashedPersonId) {
        _tupleEntry.set(HASHED_PERSON_ID_FIELD, hashedPersonId);
    }

    public Integer getHashedPersonIdField() {
        return _tupleEntry.getInteger(HASHED_PERSON_ID_FIELD);
    }

}
