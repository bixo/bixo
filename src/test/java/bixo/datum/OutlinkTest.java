package bixo.datum;


import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class OutlinkTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testIsNoFollow() throws Exception {
        Assert.assertTrue(makeOutlink("nofollow").isNoFollow());
        Assert.assertTrue(makeOutlink("other1,noFollow other2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1 noFollow,other2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1,noFollow\tother2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1\tnoFollow,other2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1  noFollow,\t\tother2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1=other1value\t\t  ,nofollow other2").isNoFollow());
        Assert.assertFalse(makeOutlink("nofollowing").isNoFollow());
    }

    private Outlink makeOutlink(String relAttributes) {
        return new Outlink( "http://www.test-domain.com/target.html",
                            "Interesting target",
                            relAttributes);
    }
}
