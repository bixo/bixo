/**
 * Copyright (c) 2009-2010 TransPac Software, Inc.
 * All rights reserved.
 *
 */
package bixo.datum;

import bixo.cascading.Datum;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Subclasses can hide fields inside the payload field tuple,
 * allowing this data to be payloaded through the workflow without
 * it having knowledge of the payload field details (though it may
 * still have to copy the payload field tuple from an instance of
 * one subclass to an instance of another).
 */
@SuppressWarnings("serial")
public class PayloadDatum extends Datum {

    public static final String PAYLOAD_FN = fieldName(PayloadDatum.class, "payload");
    public static final Fields FIELDS = new Fields(PAYLOAD_FN).append(Datum.getSuperFields(PayloadDatum.class));

    private transient Payload _payload;
    private transient boolean _updated = false;
    
    public PayloadDatum() {
        super(FIELDS);
    }
    
    public PayloadDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }

    public PayloadDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
        validateFields(fields, FIELDS);
    }

    public PayloadDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    @Override
    public void commit() {
        super.commit();

        if (_updated) {
            _tupleEntry.set(PAYLOAD_FN, _payload.toTuple());
            _updated = false;
        }
    }
    
    @Override
    public void reset() {
        setPayload((Payload)null);
    }
    
    public Payload getPayload() {
        if (_payload == null) {
            _payload = new Payload((Tuple)_tupleEntry.get(PAYLOAD_FN));
            _updated = false;
        }
        
        return _payload;
    }

    public Object getPayloadValue(String key) {
        return getPayload().get(key);
    }
    
    /**
     * Set the payload to be the passed information.
     * 
     * @param payload new payload (which can be null)
     */
    public void setPayload(Payload payload) {
        _payload = payload;
        _updated = payload != null;
    }
    
    public void setPayload(PayloadDatum datum) {
        setPayload(new Payload(datum.getPayload()));
    }
    
    public void setPayloadValue(String key, Object value) {
        getPayload().put(key, value);
        _updated = true;
    }
}
