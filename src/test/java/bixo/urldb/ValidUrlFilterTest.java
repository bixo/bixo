package bixo.urldb;

import org.junit.Assert;
import org.junit.Test;


public class ValidUrlFilterTest {

    @Test
    public void testValidUrls() {
        IUrlFilter urlFilter = new ValidUrlFilter();
        Assert.assertNotNull(urlFilter.filter("http://domain.com"));
    }
    
    @Test
    public void testInvalidUrls() {
        IUrlFilter urlFilter = new ValidUrlFilter();
        Assert.assertNull("No protocol", urlFilter.filter("www.domain.com"));
        Assert.assertNull("Unknown protocol", urlFilter.filter("mdata://www.domain.com"));
        Assert.assertNull("Invalid port", urlFilter.filter("http://www.domain.com:a"));
    }
}
