package bixo.cascading;

import junit.framework.Assert;

import org.junit.Test;

import cascading.tuple.Fields;


public class DatumTest {

    @SuppressWarnings("serial")
    private static class MyDatum extends BaseDatum {

        public static final Fields FIELDS = new Fields("a", "b");
        
        public MyDatum() {
            super(FIELDS);
        }
        
        public MyDatum(Fields fields) {
            super(fields);
            validateFields(fields, FIELDS);
        }
    }
    
    @Test
    public void testSubclassing() throws Exception {
        MyDatum md = new MyDatum();
        Assert.assertEquals(MyDatum.FIELDS, md.getFields());
    }
    
    @Test
    public void testValidation() throws Exception {
        try {
            new MyDatum(MyDatum.FIELDS);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exception");
        }
        
        try {
            new MyDatum(new Fields("c"));
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {
        }
        
        
    }
}
