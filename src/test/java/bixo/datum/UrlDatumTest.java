package bixo.datum;

import org.junit.Assert;
import org.junit.Test;


public class UrlDatumTest {
    
    @Test
    public void testFieldNames() {
        Assert.assertEquals("UrlDatum-url", UrlDatum.URL_FIELD);
    }
}
