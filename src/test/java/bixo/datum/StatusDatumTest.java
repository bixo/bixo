package bixo.datum;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class StatusDatumTest {

    @Test
    public void testConstructorWithPayload() throws Exception {
        Payload payload = new Payload();
        payload.put("key", "value");
        StatusDatum sd = new StatusDatum("url", UrlStatus.UNFETCHED, payload);
        
        assertEquals("value", sd.getPayload().get("key"));
    }
}
