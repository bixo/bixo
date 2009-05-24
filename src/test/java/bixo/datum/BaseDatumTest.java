package bixo.datum;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import cascading.tuple.Fields;


public class BaseDatumTest {
    private class MyDatum extends BaseDatum {
        private String _value;
        
        @SuppressWarnings("unchecked")
        public MyDatum(String value, Map<String, Comparable> metaData) {
            super(metaData);
            
            _value = value;
        }
        
        @Override
        protected Fields getStandardFields() {
            return new Fields("field1");
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Comparable[] getStandardValues() {
            return new Comparable[] { _value };
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testMetaDataFields() {
        Map<String, Comparable> metaData = new HashMap<String, Comparable>();
        metaData.put("key", "value");

        MyDatum datum = new MyDatum("value1", metaData);
        Fields metaDataFields = datum.getMetaDataFields();
        Assert.assertEquals("Should be one metadata field", 1, metaDataFields.size());
        Assert.assertEquals("key", metaDataFields.get(0), "key");
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testMakingMetaDataFields() {
        // first do it using the map guy
        Map<String, Comparable> metaData = new HashMap<String, Comparable>();
        metaData.put("a-key", "a-value");
        metaData.put("b-key", "b-value");
        Fields fields1 = BaseDatum.makeMetaDataFields(metaData);
        
        Fields fields2 = BaseDatum.makeMetaDataFields("b-key", "a-key");
        
        Assert.assertTrue(fields1.equals(fields2));
    }
}
