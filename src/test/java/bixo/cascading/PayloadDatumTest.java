package bixo.cascading;

import junit.framework.Assert;

import org.junit.Test;

import bixo.datum.PayloadDatum;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;


public class PayloadDatumTest {

    @SuppressWarnings("serial")
    private static class MyDatum extends PayloadDatum {
        
        public static final String A_FN = Datum.fieldName(MyDatum.class, "a");
        public static final Fields FIELDS = new Fields(A_FN).append(Datum.getSuperFields(MyDatum.class));
        
        public MyDatum() {
            super(FIELDS);
        }
        
        public MyDatum(TupleEntry tupleEntry) {
            super(tupleEntry);
        }

        public MyDatum(String a) {
            super(FIELDS);
            
            setA(a);
        }
        
        public String getA() {
            return _tupleEntry.getString(A_FN);
        }
        
        public void setA(String a) {
            _tupleEntry.set(A_FN, a);
        }
    }
    
    @Test
    public void testFieldDefinitions() throws Exception {
        Assert.assertEquals(2, MyDatum.FIELDS.size());
        MyDatum.FIELDS.verifyContains(new Fields(MyDatum.A_FN));
        Assert.assertEquals(MyDatum.FIELDS, new MyDatum().getFields());
    }

    @Test
    public void testPayloadRoundTrip() throws Exception {
        MyDatum d1 = new MyDatum("test");
        
        d1.setPayloadValue("key", "value");
        Assert.assertEquals("value", d1.getPayloadValue("key"));

        TupleEntry te = d1.getTupleEntry();
        
        MyDatum d2 = new MyDatum(te);
        
        Assert.assertEquals("value", d2.getPayloadValue("key"));
    }
    
    @Test
    public void testSettingTuple() throws Exception {
        MyDatum d1 = new MyDatum("test");
        d1.setPayloadValue("key", "value");
        Assert.assertEquals("value", d1.getPayloadValue("key"));

        MyDatum d2 = new MyDatum();
        d1.setTuple(d2.getTuple());
        Assert.assertNull(d1.getPayloadValue("key"));
    }
    
    @Test
    public void testDirectCopyOfPayload() throws Exception {
        MyDatum d1 = new MyDatum("test");
        d1.setPayloadValue("key", "value");

        MyDatum d2 = new MyDatum();
        d2.setPayload(d1);
        Assert.assertEquals("value", d2.getPayloadValue("key"));
        
        // Make sure we've got a real copy
        d1.setPayloadValue("key", "value2");
        Assert.assertEquals("value", d2.getPayloadValue("key"));
    }
}
