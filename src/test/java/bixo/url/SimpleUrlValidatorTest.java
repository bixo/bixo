package bixo.url;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import bixo.url.SimpleUrlValidator;

public class SimpleUrlValidatorTest {

    private SimpleUrlValidator _validator;
    @Before
    public void setup() {
       _validator = new SimpleUrlValidator();
    }
    
    @Test
    public void testValidate() {
        Assert.assertEquals(true, _validator.validate("http://foo.com"));
        Assert.assertEquals(true, _validator.validate("http://www.foo.com"));
        Assert.assertEquals(true, _validator.validate("http://www.foo.com/"));
        Assert.assertEquals(true, _validator.validate("http://aws.foo.com/"));
        Assert.assertEquals(true, _validator.validate("https://aws.foo.com/"));

        Assert.assertEquals(false, _validator.validate("foo.com"));
        Assert.assertEquals(false, _validator.validate("www.foo.com"));
        Assert.assertEquals(false, _validator.validate("mailto://ken@foo.com"));
        Assert.assertEquals(false, _validator.validate("mailto:?Subject=http://info.foo.com/copyright/us/details.html"));
        Assert.assertEquals(false, _validator.validate("smtp://aws.foo.com/"));
        Assert.assertEquals(false, _validator.validate("ftp://aws.foo.com/"));
        Assert.assertEquals(false, _validator.validate("javascript:foobar()"));
        
        Assert.assertEquals(false, _validator.validate("feed://getbetterhealth.com/feed"));
        Assert.assertEquals(false, _validator.validate("ttp://www.thehealthcareblog.com/the_health_care_blog/2009/07/healthcare-reform-lessons-from-mayo-clinic.html"));
   }
    
}
